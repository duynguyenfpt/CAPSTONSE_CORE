import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Test {
    public static void main(String[] args) {
        System.out.println("TEST");
        try {
            Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString("postgresql", "127.0.0.1", "5433", "sync_test", "postgres", ""));
            String query = "Select * from public.transaction_4;";
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println(rs.getInt("request_id"));
            }
            System.out.println("connected!!");
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
//        String query = "" +
//                "SELECT si.server_host,di.port,di.username,di.password,di.database_name," +
//                "tbls.`table_name`,jobs.id as job_id, str.partition_by, str.is_all, str.id as str_id FROM \n" +
//                "(SELECT * FROM webservice_test.jobs\n" +
//                "where number_retries < max_retries and status = 'pending' and deleted = 0) as jobs\n" +
//                "INNER JOIN\n" +
//                "webservice_test.sync_table_requests str\n" +
//                "INNER JOIN\n" +
//                "webservice_test.tables tbls\n" +
//                "INNER JOIN\n" +
//                "webservice_test.database_infos as di\n" +
//                "INNER JOIN\n" +
//                "webservice_test.server_infos as si\n" +
//                "on jobs.request_id = str.request_id\n" +
//                "and str.table_id = tbls.id\n" +
//                "and tbls.database_info_id = di.id \n" +
//                "and si.id = di.server_info_id;";
//        System.out.println(query);
        try {
            Connection connection = sqlUtils.getConnectionOracle("system", "oracle", "localhost", "1521", "");
            System.out.println("connected!!");
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
    }
}
