import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class IoTDBTSUDF implements IQueryWorkload{
    private IoTDBConnection ioTDBConnection;
    private Connection connection;

    private String file_name;

    private Map<String, List<String>> device_sensors;
    private String device;
    private int common_prefix_sz;

    private long MILL2NANO = 1000L;
    private long avg_interval;
    private int iteration = 30;

    // pivots
    private List<Double> valuePivot;
    private List<Long> timePivot;

    //private int[] comparable_sensor_id = {};
    private int comparable=1;

    // udf
    private Map<String, String> registeredUDFs;

    public IoTDBTSUDF(String dataset, Integer num_common) throws Exception {
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

        valuePivot = stat.findPivot(stat.stat(file_name), 10);
        timePivot = stat.findPivotTime(file_name);
        avg_interval = stat.avg_interval;
        //comparable = stat.pivotIndex;


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
            if(sensor.charAt(sensor.length()-1) == ')')
                sensor = sensor.split("\\(")[0];
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
                Arrays.asList("Q1R", "Q1OR", "Q1V", "Q2", "Q3", "Q4", "Q5", "QA1", "QA2", "QA3"));
        try {
            while(!que.isEmpty()) {
                String func = que.poll();
                List<Status> ans;
                switch (func) {
                    case "Q1R": ans = Q1_rangeQuery(); break;
                    case "Q1OR": ans = Q1_openRangeQuery(); break;
                    case "Q1V": ans = Q1_valueRangeQuery(); break;
                    case "Q2": ans = Q2_selectPartial(); break;
                    case "Q3": ans = Q3_merge2series(); break;
                    case "Q4": ans = Q4_alignRangeQuery2series(); break;
                    case "Q5": ans = Q5_aggregationByRangedWindow(); break;
                    case "QA1": ans = QA1_sampling(); break;
                    case "QA2": ans = QA2_aggregationMap(); break;
                    case "QA3": ans = QA3_alignment3series(); break;
                    default: ans = new ArrayList<>();
                }
                if(result.containsKey(func)) {
                    result.get(func).addAll(ans);
                } else {
                    result.put(func, ans);
                }
            }
        } catch (SQLException e) {
            close(); return new HashMap<>();
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
    public List<Status> Q1_rangeQuery() throws SQLException {
        String key = "ClosedRange";
        String sensor = device_sensors.get(device).get(0);
        long start = timePivot.get(0);
        long end = timePivot.get(5); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_3ary.getTemplate(),
                sensor, "'from'='"+MILL2NANO*start + "'",
                "'to'='"+MILL2NANO*end + "'");
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
        Status ans =  new Status(true, (System.nanoTime()-begin)/iteration);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q1_openRangeQuery() throws SQLException {
        String key = "OpenRange";
        //String sensor = device_sensors.get(device).get(0);
        long start = timePivot.get(3);
        long end = timePivot.get(7); // variable

        long count = 0L;
        for(String sensor: device_sensors.get(device)) {

            // udf
            String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_3ary.getTemplate(),
                    sensor, "'less_than'='"+MILL2NANO*start + "'",
                    "'greater_than'='"+MILL2NANO*end + "'");
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
            count += System.nanoTime() - begin;

        }

        Status ans =  new Status(true, (count)/((long) iteration * device_sensors.get(device).size()));
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q1_valueRangeQuery() throws SQLException {
        String key = "ClosedValue";
        String sensor = device_sensors.get(device).get(0);
        double start = valuePivot.get(0);
        double end = valuePivot.get(valuePivot.size()/2); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_3ary.getTemplate(),
                sensor, "'from'='"+ start + "'",
                "'to'='"+ end + "'");
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
        Status ans =  new Status(true, (System.nanoTime()-begin)/iteration);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q2_selectPartial() throws SQLException {
        String key = "ClosedRange";
        String sensor = device_sensors.get(device).get(0);
        long start = timePivot.get(0);
        long end = timePivot.get(9); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_3ary.getTemplate(),
                sensor, "'from'='"+MILL2NANO*start + "'",
                "'to'='"+MILL2NANO*end + "'");
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
        Status ans =  new Status(true, (System.nanoTime()-begin)/iteration);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q3_merge2series() throws SQLException {
        String key = "Merge";
        String sensor = device_sensors.get(device).get(0);
        //long start = timePivot.get(0);
        long pivot = timePivot.get(5); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                sensor, "'pivot'='"+MILL2NANO*pivot + "'");
        String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                registeredUDFs.get(key), call);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                        udf_part, device);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q4_alignRangeQuery2series() throws SQLException {
        String key = "ClosedRange";
        String sensor1 = device_sensors.get(device).get(0);
        String sensor2 = device_sensors.get(device).get(1);
        long start = timePivot.get(0);
        long end = timePivot.get(9); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_4ary.getTemplate(),
                sensor1, sensor2, "'from'='"+MILL2NANO*start + "'",
                "'to'='"+MILL2NANO*end + "'");
        String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                registeredUDFs.get(key), call);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                        udf_part, device);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q5_aggregationByRangedWindow() throws SQLException {
        String key = "AggRWindow";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        long wsz = avg_interval * 100 * MILL2NANO;
        long skip = avg_interval * MILL2NANO;
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
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> QA1_sampling() throws SQLException {
        String key = "Sample";
        String sensor = device_sensors.get(device).get(0);
        long start = timePivot.get(0);
        long end = timePivot.get(5); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                sensor, "'rate'='"+ 0.5 + "'");
        String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                registeredUDFs.get(key), call);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                        udf_part, device);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> QA2_aggregationMap() throws SQLException {
        String key = "AggMap";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_4ary.getTemplate(),
                sensor, "'func'='"+ func + "'", "'start'='"+MILL2NANO*start + "'",
                "'end'='"+MILL2NANO*end + "'");
        String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                registeredUDFs.get(key), call);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                        udf_part, device);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> QA3_alignment3series() throws SQLException {
        String key = "ClosedRange";
        String sensor1 = device_sensors.get(device).get(0);
        String sensor2 = device_sensors.get(device).get(1);
        String sensor3 = device_sensors.get(device).get(2);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        // udf
        String call = String.format(SQLTemplateIoTDB.FUNCTION_CALL_5ary.getTemplate(),
                sensor1, sensor2, sensor3, "'from'='"+ start + "'",
                "'to'='"+ end + "'");
        String udf_part = String.format(SQLTemplateIoTDB.IOTDB_UDF_FUNCTION.getTemplate(),
                registeredUDFs.get(key), call);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                        udf_part, device);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<5;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/5);
        return Collections.singletonList(ans);
    }
}
