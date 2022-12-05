import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IQueryWorkload extends IQueryBenchmark {

    @Override
    void init_workload() throws Exception;

    @Override
    Map<String, List<Status>> run() throws Exception;

    @Override
    void close() throws Exception;

    @Override
    String getDevice();

    List<Status> Q1_rangeQuery() throws SQLException;

    List<Status> Q1_openRangeQuery() throws SQLException;

    List<Status> Q1_valueRangeQuery() throws SQLException;

    List<Status> Q2_selectPartial() throws SQLException;

    List<Status> Q3_merge2series() throws SQLException;

    List<Status> Q4_alignRangeQuery2series() throws SQLException;

    List<Status> Q5_aggregationByRangedWindow() throws SQLException;

    List<Status> QA1_sampling() throws SQLException;

    List<Status> QA2_aggregationMap() throws SQLException;

    List<Status> QA3_alignment3series() throws SQLException;
}
