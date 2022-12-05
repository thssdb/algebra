import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        // E:\projects\iotdb\apache-iotdb-0.13.3-all-bin\sbin
        IQueryBenchmark queryWorkload;
        String prefix = "E:\\projects\\iotdb\\dataset\\";

        String[] datasets = new String[]
                {//"Atomosphere\\wind.csv",
                 //"Climate\\climate_sc\\iot.climate.csv",
                 "Ship\\iot.ship.csv",
                 //"WindStat\\iot.windStat2.csv",
                  //     "Open.RSSI\\rssi.csv",
                    //    "Open.Bitcoin\\bitcoin.csv"
                    //    "Open.gas\\gas.csv",
                    //    "open.aq\\aq.csv",
                    //    "gen.noise\\noise.csv",
                     //   "gen.sin\\sin.csv"
        };

        for(String dataset_post: datasets) {

            String dataset = prefix + dataset_post;
            String output_path = "./latency-ts/udf/";

            queryWorkload = new IoTDBTSUDF(dataset, 3);
            //queryWorkload = new IoTDBTS(dataset, 3);
            //queryWorkload = new IoTDBTemporalUDF(dataset, 3);
            //queryWorkload = new IoTDBTemporal(dataset, 3);
            //queryWorkload = new IoTDBRelationalUDF(dataset, 3);
            //queryWorkload = new IoTDBRelational(dataset, 3);

            String device = queryWorkload.getDevice();
            String output_file = output_path + device + ".txt";

            try {
                Map<String, List<Status>> result = queryWorkload.run();
                write(result, output_file);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void write(Map<String, List<Status>> result, String output) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        for(String k: result.keySet()) {
            writer.write(k + ", ");
            for(Status s: result.get(k)) {
                writer.write(String.valueOf(s.getTimeCost()));
                writer.write(", ");
            }
            writer.write("\n");
        }
        writer.close();
    }
}
