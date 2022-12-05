import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.BinaryOperator;

public class IoTDBTemporal implements ITemporalQuery {
    private IoTDBConnection ioTDBConnection;
    private Connection connection;

    private String file_name;

    private Map<String, List<String>> device_sensors;
    private String device;
    private int common_prefix_sz;

    private long MILL2NANO = 1000L;
    private long avg_interval;
    private int iteration = 3;

    private int count_wsz = 10;

    // pivots
    private List<Double> valuePivot;
    private List<Long> timePivot;
    private List<Long> timeLine;


    // udf
    private Map<String, String> registeredUDFs;

    public IoTDBTemporal(String dataset, Integer num_common) throws Exception {
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
        timeLine = stat.timeLine;

        //init_T1();
    }

    private void init_T1() throws SQLException {
        // add tag
        for(int i=0;i<timeLine.size();i++) {
            int tag = (int) Math.floor(i*1.0/count_wsz);

            String contents = String.format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                    "Time", "tag");
            String head = String
                    .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                            device, contents);
            String values = String
                    .format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                            timeLine.get(i), tag);
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_INSERT.getTemplate(),
                            head, values);

            Statement stmt = connection.createStatement();
            stmt.execute(sql);
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
        String func = "AVG"; //variable
        long wsz = count_wsz;
        long skip = count_wsz;

        long cost = 0;
        int cnt = 0;



        for(String sensor: device_sensors.get(device)) {
            cnt += 1;
            // stmt

            Statement stmt = connection.createStatement();
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor, device);

            ResultSet rs = stmt.executeQuery(sql);
            for(int i = 0;i< iteration ;i++){
                long begin = System.nanoTime();
                long tmp = rs.getLong("Time");
                double curr = rs.getDouble(device + "." + sensor);
                //cnt = 1;
                List<Double> ans = new ArrayList<>();
                List<Double> cache = new ArrayList<>();
                do {
                    if (cache.size() == wsz) {
                        ans.add(cache.stream().reduce(0.d, new BinaryOperator<Double>() {
                            @Override
                            public Double apply(Double aDouble, Double aDouble2) {
                                return aDouble + aDouble2;
                            }
                        }) / wsz);
                        cache = new ArrayList<>();
                    } else cache.add(curr);
                } while (rs.next());
                cost += System.nanoTime() - begin;
            }

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt*iteration));
        return Collections.singletonList(ans);
    }

    public List<Double> init_pattern(int len) {
        List<Double> pattern = new ArrayList<>();
        Random rd = new Random();
        for (int i = 0; i < len; i++) pattern.add(rd.nextDouble());
        return pattern;
    }

    @Override
    public List<Status> QT2_patternMatching() throws SQLException {
        String key = "pattern";
        //String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "AVG"; //variable
        int wsz = count_wsz;
        long skip = count_wsz;

        long cost = 0;
        int cnt = 0;



        for(String sensor: device_sensors.get(device)) {
            cnt += 1;
            // stmt

            List<Double> pattern = init_pattern(wsz);

            Statement stmt = connection.createStatement();
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor, device);

            ResultSet rs = stmt.executeQuery(sql);
            for(int i=0;i<iteration;i++) {
                long begin = System.nanoTime();
                long tmp = rs.getLong("Time");
                double curr = rs.getDouble(device + "." + sensor);
                cnt = 1;
                List<Double> ans = new ArrayList<>();
                List<Double> cache = new ArrayList<>();
                do {
                    if (cache.size() == wsz) {
                        ans.add(cache.stream().reduce(0.d, new BinaryOperator<Double>() {
                            @Override
                            public Double apply(Double aDouble, Double aDouble2) {
                                return aDouble + aDouble2;
                            }
                        }) / wsz);
                        cache = new ArrayList<>();
                    } else {
                        double x = curr - pattern.get(cache.size());
                        cache.add(x);
                    }
                } while (rs.next());
                cost += System.nanoTime() - begin;
            }


            /*
            String head = String
                    .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                            func, sensor);
            String from = device;
            String groupBy = String
                    .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                            "TAGS", "tag");

            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_GROUP.getTemplate(),
                            head, from, groupBy);
             */

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt*iteration));
        return Collections.singletonList(ans);
    }
}
