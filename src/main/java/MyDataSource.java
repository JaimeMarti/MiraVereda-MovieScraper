import oracle.jdbc.pool.OracleDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

public class MyDataSource {
    public static DataSource getMyDataSource() {
        try {
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL("jdbc:oracle:thin:@localhost:1539:xe");
            dataSource.setUser("system");
            dataSource.setPassword("grupo7");
            return dataSource;
        }
        catch(SQLException err) {
            err.printStackTrace();
            return null;
        }
    }

}
