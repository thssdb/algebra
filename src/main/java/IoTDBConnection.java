import org.apache.iotdb.jdbc.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class IoTDBConnection {
    protected static final String JDBC_URL = "jdbc:iotdb://%s:%s/";
    private Connection connection;
    //private AtomicInteger currConnectionIndex = new AtomicInteger(0);

    public IoTDBConnection() { init();}

    public void init() {
        int nodeSize = 1;
        String url = String.format(JDBC_URL, "127.0.0.1", "6667");
        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable = false;
            connection =
                    DriverManager.getConnection(url, "root", "root");
        } catch (Exception e) {

        }
    }

    public void close() throws SQLException {
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }

}
