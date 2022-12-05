import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IRelationalQuery extends IQueryBenchmark {
    @Override
    void init_workload() throws Exception;

    @Override
    Map<String, List<Status>> run() throws Exception;

    @Override
    void close() throws Exception;

    @Override
    String getDevice();

    List<Status> QR1_SimilarityJoin() throws SQLException;

    List<Status> QR2_SeriesDictionaryJoin() throws Exception;

    List<Status> QR3_AggByEvent() throws SQLException;

    List<Status> QR4_SimilarityAlignMulti() throws SQLException;

}
