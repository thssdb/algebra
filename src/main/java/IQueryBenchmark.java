import java.util.List;
import java.util.Map;

public interface IQueryBenchmark {

    void init_workload() throws Exception;

    Map<String, List<Status>> run() throws Exception;

    void close() throws Exception;

    String getDevice();

}
