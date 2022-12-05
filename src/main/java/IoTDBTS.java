import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class IoTDBTS implements IQueryWorkload{
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
    private int count;

    // udf
    private Map<String, String> registeredUDFs;

    public IoTDBTS(String dataset, Integer num_common) throws Exception {
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

        valuePivot = stat.findPivot(stat.stat(file_name), stat.cntx);
        count = stat.cntx;
        timePivot = stat.findPivotTime(file_name);
        avg_interval = stat.avg_interval;
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
    public Map<String, List<Status>> run() throws SQLException {
        Map<String, List<Status>> result = new HashMap<>();
        Deque<String> que = new ArrayDeque<>(
                Arrays.asList("Q1R", "Q1OR", "Q1V", "Q2", "Q3", "Q4", "Q5", "QA1", "QA2", "QA3"));
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
    public List<Status> Q1_rangeQuery() throws SQLException {
        String key = "ClosedRange";
        //String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(5); // variable
        // sfw query
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);

        // stmt
        long count = 0L;
        for(String sensor: device_sensors.get(device)) {
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                            sensor, device, call);
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
    public List<Status> Q1_openRangeQuery() throws SQLException {
        String key = "OpenRange";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(3);
        long end = MILL2NANO*timePivot.get(7); // variable
        // sfw query
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", end);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", start);
        String call = String
                .format(SQLTemplateIoTDB.DISJUNCTION.getTemplate(),
                        geq, less);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        sensor, device, call);
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
    public List<Status> Q1_valueRangeQuery() throws SQLException {
        String key = "ClosedValue";
        String sensor = device_sensors.get(device).get(0);
        double start = valuePivot.get(0);
        double end = valuePivot.get(valuePivot.size()/2); // variable
        // sfw query
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        sensor, start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        sensor, end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        sensor, device, call);
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
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable

        // sfw query
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        sensor, device, call);
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

        // sfw
        long pivot = timePivot.get(5); // variable
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", pivot);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", pivot);

        // stmt
        String sql1 = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        sensor, device, less);
        String sql2 = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        sensor, device, geq);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql1 + "; " + sql2);
        long begin = System.nanoTime();
        for(int i=0;i<iteration;i++) {
            stmt.execute(sql1);stmt.execute(sql2);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/iteration);
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> Q4_alignRangeQuery2series() throws SQLException {
        String key = "ClosedRange";
        String sensor1 = device_sensors.get(device).get(0);
        String sensor2 = device_sensors.get(device).get(1);
        long start = timePivot.get(0);
        long end = timePivot.get(9); // variable

        // sfw
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);
        String head = String
                .format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                        sensor1, sensor2);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        head, device, call);
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
    public List<Status> Q5_aggregationByRangedWindow() throws SQLException {
        String key = "AggRWindow";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        long wsz = (long)(avg_interval * 100);
        long skip = avg_interval * MILL2NANO;

        // sfw
        String rng = String
                .format(SQLTemplateIoTDB.RANGE.getTemplate(),
                        start, end);
        String call = String
                .format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                        rng, wsz+"ms");
        String groupBySubC = String
                .format(SQLTemplateIoTDB.PRC.getTemplate(),
                        call);
        String func_call = String
                .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                        func, sensor);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_GROUP.getTemplate(),
                        func_call, device, groupBySubC);
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
    public List<Status> QA1_sampling() throws SQLException {
        String key = "AggRWindow";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        long wsz = (long)(avg_interval * 100);
        long skip = avg_interval * MILL2NANO;

        // sfw
        String rng = String
                .format(SQLTemplateIoTDB.RANGE.getTemplate(),
                        start, end);
        String call = String
                .format(SQLTemplateIoTDB.FUNCTION_CALL_2ary.getTemplate(),
                        rng, wsz+"ms");
        String groupBySubC = String
                .format(SQLTemplateIoTDB.PRC.getTemplate(),
                        call);
        String func_call = String
                .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                        func, sensor);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_GROUP.getTemplate(),
                        func_call, device, groupBySubC);
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
    public List<Status> QA2_aggregationMap() throws SQLException {
        String key = "AggMap";
        String sensor = device_sensors.get(device).get(0);
        long start = MILL2NANO*timePivot.get(0);
        long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg";
        // sfw query
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);
        String func_call = String
                .format(SQLTemplateIoTDB.FUNC_PRC.getTemplate(),
                        func, sensor);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        func_call, device, call);
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
    public List<Status> QA3_alignment3series() throws SQLException {
        String key = "ClosedRange";
        String sensor1 = device_sensors.get(device).get(0);
        String sensor2 = device_sensors.get(device).get(1);
        String sensor3 = device_sensors.get(device).get(2);
        long start = timePivot.get(0);
        long end = timePivot.get(9); // variable

        // sfw
        String geq = String
                .format(SQLTemplateIoTDB.GREATER_EQ_THAN.getTemplate(),
                        "time", start);
        String less = String
                .format(SQLTemplateIoTDB.LESS_THAN.getTemplate(),
                        "time", end);
        String call = String
                .format(SQLTemplateIoTDB.CONJUNCTION.getTemplate(),
                        geq, less);
        String head = String
                .format(SQLTemplateIoTDB.FUNCTION_CALL_3ary.getTemplate(),
                        sensor1, sensor2, sensor3);

        // stmt
        String sql = String
                .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM_WHERE.getTemplate(),
                        head, device, call);
        Statement stmt = connection.createStatement();
        System.out.println("Executing SQL: " + sql);
        long begin = System.nanoTime();
        for(int i=0;i<iteration;i++) {
            stmt.execute(sql);
        }
        Status ans =  new Status(true, (System.nanoTime()-begin)/iteration);
        return Collections.singletonList(ans);
    }
}
