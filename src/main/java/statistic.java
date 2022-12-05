import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class statistic {
    public final double plus = 100d;
    public int cntx;
    public long avg_interval;
    public long min_interval;

    public boolean check_type = true;

    private String dataset;

    public List<Long> timeLine = new ArrayList<>();


    public void make_stat(String filePath) throws FileNotFoundException {
        File f = new File(filePath);
        Scanner sc = new Scanner(f);
        Map<Integer, Double> ans = new HashMap<>();
        int cnt = 0;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] res = line.split(",");
            if (res[0].equals("Time")) {
                if (!check_type) continue;
                for (int i = 1; i < res.length; i++) {
                    String cur = res[i];
                    char p = cur.charAt(res[i].length() - 2);
                    if (p == 'X') continue;
                    else {
                        //pivotIndex = i;
                        break;
                    }
                }
            }
        }
    }



    public Map<Integer, Double> stat(String filePath) throws FileNotFoundException {
        File f = new File(filePath);
        Scanner sc = new Scanner(f);
        Map<Integer, Double> ans = new HashMap<>();
        int cnt = 0;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] res = line.split(",");
            if(res[0].equals("Time")) {
                continue;
            }
            double x = 0.d;
            try {
                x = Double.parseDouble(res[1]);
            } catch (Exception e) {
                continue;
            }
            Integer s = (int) Math.ceil(x*plus);
            //Integer prec = Math.ceil(x*10.0);
            if(ans.containsKey(s)) ans.replace(s, ans.get(s) + 1);
            else ans.put(s, 1d);
            cnt ++;
        }
        cntx = cnt;
        //for(Integer x: ans.keySet()) ans.replace(x, ans.get(x));
        return ans;
    }

    public List<Double> findPivot(Map<Integer, Double> in, int count) {
        List<Integer> vals = new ArrayList<>(in.keySet());
        vals.sort(Comparator.naturalOrder());
        List<Double> pivot = new ArrayList<>();
        int prev = 0; double tot=0;
        pivot.add(vals.get(0)*1.0/this.plus);
        for(int i=0;i<vals.size();i++) {
            Integer x = vals.get(i);
            tot += in.get(x);
            int ratio = (int) Math.floor(tot*10.0 /count);
            if(ratio > prev) pivot.add(x*1.0/this.plus);
            prev=  ratio;
        }
        //pivot.add(vals.get(vals.size()-1)*1.0/this.plus);
        return pivot;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    private long castTime(String date) throws ParseException {
        switch (dataset) {
            case "root.iot.atom":
            case "root.iot.ship":
            case "root.iot.windStat":
            case "root.open.rssi":
            case "root.open.aq":
            case "root.open.btc":
            case "root.open.gas":
            case "root.gen.noise":
            case "root.gen.sin":
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date d = format.parse(date.split("\\.")[0]);
                return d.getTime();
            case "root.iot.climate":
            default:
                return Long.parseLong(date);
        }
    }

    public List<Long> findPivotTime(String filePath) throws FileNotFoundException, ParseException {
        File f = new File(filePath);
        Scanner sc = new Scanner(f);
        Map<Integer, Double> ans = new HashMap<>();
        int cnt = 0;
        long min=-1, max=-1;
        min_interval = Long.MAX_VALUE;
        long prev = -1;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] res = line.split(",");
            if(res[0].equals("Time")) continue;
            //SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //Date d = format.parse(res[0].split("\\.")[0]);
            //long t = Long.parseLong(res[0]);
            //long t = d.getTime();
            long t = castTime(res[0]);
            timeLine.add(t);
            if(prev == -1) prev = t;
            else {
                min_interval = Math.min(min_interval, Math.abs(prev - t));
                prev = t;
            }
            if(min == -1) min = t;
            min = Math.min(min, t);
            max = Math.max(max, t);
            //Integer prec = Math.ceil(x*10.0);
            cnt ++;
        }
        cntx = cnt;
        List<Long> res = new ArrayList<>();
        for(int i=0;i<=10;i++) {
            res.add(min + (max-min)*i/10);
        }
        //for(Integer x: ans.keySet()) ans.replace(x, ans.get(x));
        avg_interval = (max-min)/cnt;
        return res;
    }

    public static void main(String[] args) throws FileNotFoundException {
        statistic s = new statistic();
        List<Double> ans = s.findPivot(s.stat("./dataset/climate_sc/t70.sc5.csv"), s.cntx);
        for(Double x: ans) System.out.println(x);
    }
}
