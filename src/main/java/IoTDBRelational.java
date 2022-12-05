import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class IoTDBRelational implements IRelationalQuery{
    private IoTDBConnection ioTDBConnection;
    private Connection connection;

    private String file_name;

    private Map<String, List<String>> device_sensors;
    private String device;
    private int common_prefix_sz;

    private long MILL2NANO = 1000L;
    private long avg_interval;
    private int iteration = 3;
    private Random rand = new Random(System.currentTimeMillis());

    // pivots
    private List<Double> valuePivot;
    private List<Long> timePivot;


    // udf
    private Map<String, String> registeredUDFs;

    public IoTDBRelational(String dataset, Integer num_common) throws Exception {
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
        avg_interval = stat.min_interval;
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
                Arrays.asList("QR1", "QR2", "QR3", "QR4"));
        try {
            while(!que.isEmpty()) {
                String func = que.poll();
                List<Status> ans;
                switch (func) {
                    case "QR1": ans = QR1_SimilarityJoin(); break;
                    case "QR2": ans = QR2_SeriesDictionaryJoin(); break;
                    case "QR3": ans = QR3_AggByEvent(); break;
                    case "QR4": ans = QR4_SimilarityAlignMulti(); break;
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
    public List<Status> QR1_SimilarityJoin() throws SQLException {
        String key = "SimJoin";
        //String sensor = device_sensors.get(device).get(0);
        //long start = MILL2NANO*timePivot.get(0);
        //long end = MILL2NANO*timePivot.get(9); // variable
        //String func = "avg"; //variable
        double eps = MILL2NANO * avg_interval * 2d;
        //long wsz = 100;
        //long skip = 1;

        long cost = 0;
        int cnt = 0;

        for(String sensor1: device_sensors.get(device)) {
            cnt += 1;
            // udf
            String sensor2 = device_sensors.get(device).get(rand.nextInt(device_sensors.get(device).size()));
            Statement stmt = connection.createStatement();
            String sql1 = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor1, device);
            ResultSet rs1 = stmt.executeQuery(sql1);

            String sql2 = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor2, device);
            ResultSet rs2 = stmt.executeQuery(sql2);

            for(int i = 0;i< iteration ;i++){
                long begin = System.nanoTime();

                //cnt = 1;
                List<Long> ans = new ArrayList<>();
                Deque<Long> cache = new ArrayDeque<>();
                do {
                    long tmp1 = rs1.getLong("Time");
                    long tmp2 = rs2.getLong("Time");
                    cache.add(tmp1);
                    if(Math.abs(tmp1 - tmp2) < eps) ans.add(tmp1);
                    if(cache.size() == 5) {
                        cache.pop();
                    }
                } while (rs1.next());
                cost += System.nanoTime() - begin;
            }

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt *iteration));
        return Collections.singletonList(ans);
    }

    public Map<Integer, Double> generate_dictionary(String full_sensor_name) throws FileNotFoundException {
        Map<Integer, Double> dictionary = new HashMap<>();
        List<Integer> keys = new ArrayList<>();
        File f = new File(file_name);
        Scanner sc = new Scanner(f);
        int pos = -1;
        while(sc.hasNext()) {
            String[] line = sc.nextLine().split(",");
            if(line[0].equals("Time")) {
                for(int i=1;i<line.length;i++) {
                    if (full_sensor_name.equals(line[i])) {
                        pos = i; break;
                    }
                }
            } else {
                if(pos == -1) {
                    System.out.println("Cannot find the sensor, check path.");
                    break;
                }
                try {
                    Integer x = (int) Double.parseDouble(line[pos])*1000;
                    if (!dictionary.containsKey(x)) {
                        dictionary.put(x, rand.nextDouble());
                        keys.add(x);
                    }
                } catch (Exception e) {
                    //
                }
            }
        }
        return dictionary;
    }

    @Override
    public List<Status> QR2_SeriesDictionaryJoin() throws Exception {
        String key = "RelJoinDict";
        //String sensor = device_sensors.get(device).get(0);
        //long start = MILL2NANO*timePivot.get(0);
        //long end = MILL2NANO*timePivot.get(9); // variable
        //long wsz = 100;
        //long skip = 1;

        long cost = 0;
        int cnt = 0;

        for(String sensor: device_sensors.get(device)) {
            cnt += 1;
            // stmt

            Map<Integer, Double> dict = generate_dictionary(device + "." + sensor);

            Statement stmt = connection.createStatement();
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor, device);

            ResultSet rs = stmt.executeQuery(sql);
            for(int i = 0;i< iteration ;i++){
                long begin = System.nanoTime();
                //long tmp = rs.getLong("Time");
                //double curr = rs.getDouble(device + "." + sensor);
                //cnt = 1;
                List<Double> ans = new ArrayList<>();
                //List<Double> cache = new ArrayList<>();
                do {
                    double curr = rs.getDouble(device + "." + sensor);
                    int k = (int) curr * 1000;
                    ans.add(dict.get(k));
                } while (rs.next());
                cost += System.nanoTime() - begin;
            }

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt *iteration));
        return Collections.singletonList(ans);
    }

    public List<Temporal> generate_event(int fraction, int num_event) {
        long st = timePivot.get(0), ed = timePivot.get(9);
        long interval = (ed-st)/fraction;
        List<Temporal> tmp = new ArrayList<>();
        for(int i=0;i<fraction;i++) {
            long pst = st + i*interval;
            long ped = st + (i+1)* interval;
            int event = rand.nextInt(num_event);
            tmp.add(new Temporal(pst, ped, event));
        }
        return tmp;
    }

    class Temporal {
        long st, ed;
        int tag;
        Temporal(long s, long e, int t) {st = s; ed = e; tag = t;}
    }

    @Override
    public List<Status> QR3_AggByEvent() throws SQLException {
        String key = "AggEvent";
        //String sensor = device_sensors.get(device).get(0);
        //long start = MILL2NANO*timePivot.get(0);
        //long end = MILL2NANO*timePivot.get(9); // variable
        String func = "avg"; //variable
        int fraction = 20;
        int num_event = 10;
        //long wsz = 100;
        //long skip = 1;
        List<Temporal> event = generate_event(fraction,num_event);

        long cost = 0;
        int cnt = 0;

        for(String sensor: device_sensors.get(device)) {
            cnt += 1;
            Statement stmt = connection.createStatement();
            String sql = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor, device);

            ResultSet rs = stmt.executeQuery(sql);

            for(int i = 0;i< iteration ;i++){
                long begin = System.nanoTime();
                //long tmp = rs.getLong("Time");
                //double curr = rs.getDouble(device + "." + sensor);
                //cnt = 1;
                List<Double> ans = new ArrayList<>();
                Map<Integer, Double> cache = new HashMap<>();
                do {
                    long tmp = rs.getLong("Time");
                    double curr = rs.getDouble(device + "." + sensor);
                    for(Temporal temp: event) {
                        if(tmp >= temp.st && tmp < temp.ed) {
                            if(!cache.containsKey(temp.tag)) {
                                cache.put(temp.tag, 0.d);
                            }
                            cache.replace(temp.tag, cache.get(temp.tag) + curr);
                        }
                    }
                } while (rs.next());
                cost += System.nanoTime() - begin;
            }

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt *iteration));
        return Collections.singletonList(ans);
    }

    @Override
    public List<Status> QR4_SimilarityAlignMulti() throws SQLException {
        String key = "SimJoin";
        //String sensor = device_sensors.get(device).get(0);
        //long start = MILL2NANO*timePivot.get(0);
        //long end = MILL2NANO*timePivot.get(9); // variable
        //String func = "avg"; //variable
        double eps = MILL2NANO * avg_interval * 2d;
        //long wsz = 100;
        //long skip = 1;

        long cost = 0;
        int cnt = 0;

        for(String sensor1: device_sensors.get(device)) {
            cnt += 1;
            // udf
            String sensor2 = device_sensors.get(device).get(rand.nextInt(device_sensors.get(device).size()));
            String sensor3 = device_sensors.get(device).get(rand.nextInt(device_sensors.get(device).size()));

            Statement stmt = connection.createStatement();
            String sql1 = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor1, device);
            ResultSet rs1 = stmt.executeQuery(sql1);

            String sql2 = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor2, device);
            ResultSet rs2 = stmt.executeQuery(sql2);

            String sql3 = String
                    .format(SQLTemplateIoTDB.IOTDB_SELECT_FROM.getTemplate(),
                            sensor3, device);
            ResultSet rs3 = stmt.executeQuery(sql3);

            for(int i = 0;i< iteration ;i++){
                long begin = System.nanoTime();

                //cnt = 1;
                List<Long> ans = new ArrayList<>();
                Deque<Long> cache = new ArrayDeque<>();
                do {
                    long tmp1 = rs1.getLong("Time");
                    long tmp2 = rs2.getLong("Time");
                    long tmp3 = rs3.getLong("Time");
                    cache.add(tmp1);
                    if(Math.abs(tmp1 - tmp2) < eps &&
                            Math.abs(tmp1 - tmp3) < eps &&
                            Math.abs(tmp3 - tmp2) < eps) ans.add(tmp1);
                    if(cache.size() == 5) {
                        cache.pop();
                    }
                } while (rs1.next());
                cost += System.nanoTime() - begin;
            }

            if(cnt >= 10) break;
        }

        Status ans =  new Status(true, (cost)/((long) cnt *iteration));
        return Collections.singletonList(ans);
    }
}
