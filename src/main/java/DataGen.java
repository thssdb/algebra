import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class DataGen {
    public void varied_freq(Double freq, String fileName, String write) throws IOException {
        File f = new File(fileName);
        Scanner sc = new Scanner(f);
        FileWriter fw = new FileWriter(new File(write));
        Random r = new Random();
        int pos = 0;
        while ((sc.hasNext())) {
            String next = sc.nextLine() + "\n";
            if(pos ==0) {pos=1; fw.write(next); continue;}
            if(r.nextDouble() < freq) {
                fw.write(next);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        DataGen dg = new DataGen();
        double[] cand = {1.0, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001};
        int pos = 1;
        for(double freq: cand) {
            dg.varied_freq(freq, "./dataset/climate_sc/t70.sc10.csv", "./dataset/climate_fq/t70.fq"+pos+".csv");
            dg.varied_freq(freq, "./dataset/climate_sc/t_70rat=0.10.csv", "./dataset/climate_fq/t_70fq"+pos+".csv");
            dg.varied_freq(freq, "./dataset/climate_sc/t_70rat=0.10-ver.csv", "./dataset/climate_fq/t_70fq"+pos+"-ver.csv");
            pos ++;
        }

    }
}
