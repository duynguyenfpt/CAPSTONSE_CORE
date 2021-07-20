package sub_service;

import models.TableMonitor;
import utils.db_utils.sqlUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CheckReadinessService {
    public static void main(String[] args) throws SQLException {
        Connection connection = sqlUtils.getConnection(
                sqlUtils.getConnectionString("localhsot", "3306", "cdc", "duynt", "Capstone123@")
        );
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM cdc.table_monitor WHERE is_actvie = 1 and is_ready = 1");
        while (rs.next()) {
            TableMonitor tm = new TableMonitor();
            tm.setId(rs.getInt("id"));
            tm.setHost(rs.getString("host"));
            tm.setPort(rs.getString("port"));
            tm.setTable(rs.getString("table"));
        }
    }
}
