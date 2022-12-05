import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ITemporalQuery extends IQueryBenchmark{
    @Override
    void init_workload() throws Exception;

    @Override
    Map<String, List<Status>> run() throws Exception;

    @Override
    void close() throws Exception;

    String getDevice();

    List<Status> QT1_countWindowQuery() throws SQLException;

    List<Status> QT2_patternMatching() throws SQLException;
}
