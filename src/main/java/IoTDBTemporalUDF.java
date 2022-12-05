import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class IoTDBTemporalUDF implements ITemporalQuery {
    private IoTDBConnection ioTDBConnection;
    private Connection connection;

    private String file_name;

    private Map<String, List<String>> device_sensors;
    private String device;
    private int common_prefix_sz;

    private long MILL2NANO = 1000L;
    private long avg_interval;
    private int iteration = 3;

    // pivots
    private List<Double> valuePivot;
    private List<Long> timePivot;


    // udf
    private Map<String, String> registeredUDFs;

    public IoTDBTemporalUDF(String dataset, Integer num_common) throws Exception {
        file_name = dataset;
        common_prefix_sz = num_common;
        init_device();
        ioTDBConnection = new IoTDBConnection();
        ioTDBConnection.init();
        connection = ioTDBConnection.getConnection();
        init_workload();
    }


    @Override
    public void init_workload() throws Exception {
        statistic stat = new statistic();
        stat.setDataset(device);

        //valuePivot = stat.findPivot(stat.stat(file_name), 10);
        timePivot = stat.findPivotTime(file_name);
        avg_interval = stat.avg_interval;


        String[] udfPost = new String[]{"AggCWindow", "AggEvent", "AggMap", "AggRWindow", "ClosedRange", "ClosedValue", "Merge", "OpenRange", "OpenValue", "PatternCWindow", "PatternRWindow", "RelJoinDict", "RelJoinTemporal", "Sample", "SimJoin"};
        registeredUDFs = new HashMap<>();
        for (String s : udfPost) {
            Statement stmt = connection.createStatement();
            String func_name = "udf_" + s;
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_CREATE_UDF.getTemplate(),
                            func_name,
                            "org.apache.iotdb.library.query.UDTF" + s);
            System.out.println("Running UDF register: " + sql);
            stmt.execute(sql);
            registeredUDFs.put(s, func_name);
        }
    }

    public void init_device() throws FileNotFoundException {
        File f = new File(file_name);
        Scanner sc = new Scanner(f);
        String header = sc.nextLine();
        String[] path = header.split(",");
        device_sensors = new HashMap<>();
        List<String> sensors = new ArrayList<>();
        boolean ddone = false;
        for(String pat: path) {
            if(pat.equals("Time")) continue;
            String[] x = pat.split("\\.");
            String sensor = compose(x, this.common_prefix_sz, x.length);
            sensors.add(sensor);
            if(!ddone) {
                device = compose(x, 0, this.common_prefix_sz);
                ddone = true;
            }
        }
        device_sensors.put(device, sensors);
    }

    private String compose(String[] ref, int st, int ed) {
        for(String i: ref) System.out.print(i + " ");
        System.out.println();
        StringBuilder sb = new StringBuilder();
        sb.append(ref[st]);
        st ++;
        for(int i=st;i<ed;i++) {
            sb.append(".").append(ref[i]);
        }
        return sb.toString();
    }

    @Override
    public Map<String, List<Status>> run() throws Exception {
        Map<String, List<Status>> result = new HashMap<>();
        Deque<String> que = new ArrayDeque<>(
                Arrays.asList("QT1", "QT2"));
        try {
            while(!que.isEmpty()) {
                String func = que.poll();
                List<Status> ans;
                switch (func) {
                    case "QT1": ans = QT1_countWindowQuery(); break;
                    case "QT2": ans = QT2_patternMatching(); break;
                    default: ans = new ArrayList<>();
                }
                if(result.containsKey(func)) {
                    result.get(func).addAll(ans);
                } else {
                    result.put(func, ans);
                }
            }
        } catch (SQLException e) {
            close(); return result;
        }
        close();
        return result;
    }

    @Override
    public void close() throws Exception {
        for(String s: registeredUDFs.keySet()) {
            Statement stmt = connection.createStatement();
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_DROP_UDF.getTemplate(),
                            registeredUDFs.get(s));
            stmt.execute(sql);
        }
        connection.close();
    }

    @Override
    public String getDevice() {
        return device;
    }

    @Override
    public List<Status> QT1_countWindowQuery() throws SQLException {
        String key = "AggCWindow";
        //String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        long wsz = 100;
        long skip = 1;

        long cost = 0;
        int cnt = 0;

        for(String sensor: device_sensors.get(device)) {
            cnt += 1;
            // udf
            String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_6ary.getTemplate(),
                    sensor, "'func'='"+ func + "'",
                    "'window'='"+ wsz + "'", "'skip'='"+ skip + "'",
                    "'start'='"+ start + "'",
                    "'end'='"+ end + "'");
            String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                    registeredUDFs.get(key), call);

            // stmt
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            udf_part, device);
            Statement stmt = connection.createStatement();
            System.out.println("Executing SQL: " + sql);
            long begin = System.nanoTime();
            for(int i=0;i<iteration;i++) {
                stmt.execute(sql);
            }
            cost += System.nanoTime() - begin;
            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt *iteration));
        return Collections.singletonList(ans);
    }

    public String init_pattern(int len) {
        List<Double> pattern = new ArrayList<>();
        Random rd = new Random();
        for (int i = 0; i < len; i++) pattern.add(rd.nextDouble());
        StringBuilder sb = new StringBuilder();
        sb.append(pattern.get(0));
        for(int i=1;i<pattern.size();i++) sb.append(":").append(pattern.get(i));
        return sb.toString();
    }

    @Override
    public List<Status> QT2_patternMatching() throws SQLException {
        String key = "PatternRWindow";
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        int pattern_sz = 50;
        long wsz = avg_interval * 2 * pattern_sz * MILL2NANO;
        long skip = avg_interval * MILL2NANO;


        long cost = 0;
        int cnt = 0;

        for(String sensor: device_sensors.get(device)) {
            String pattern = init_pattern(pattern_sz);
            cnt += 1;

            // udf
            String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_4ary.getTemplate(),
                    sensor, "'pattern'='"+ pattern + "'",
                    "'window'='"+ wsz + "'", "'skip'='"+ skip + "'");
            String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                    registeredUDFs.get(key), call);

            // stmt
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            udf_part, device);
            Statement stmt = connection.createStatement();
            System.out.println("Executing SQL: " + sql);
            long begin = System.nanoTime();
            for(int i=0;i<iteration;i++) {
                stmt.execute(sql);
            }
            cost += System.nanoTime() - begin;
            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt*iteration));
        return Collections.singletonList(ans);
    }
}
