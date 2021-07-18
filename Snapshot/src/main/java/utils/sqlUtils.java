package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class sqlUtils {
    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", host, port, db, userName, password);
    }

    //
    public static Connection getConnection(String dbURL) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    //
    public static void insertJobLog(String job_id, int step_id, String step_name, int status) throws SQLException {
        Connection connection = getConnection(getConnectionString("localhost", "3306",
                "synchronization", "duynt", "Capstone123@"));
        String command = "Insert into synchronization.job_log(job_id,step_id,step_name,status,number_steps,status_name) " +
                "values (?,?,?,?,?,?)";
        int numberSteps = 5;
        String statusName = "Success";
        if (status == 0) {
            statusName = "Failed";
        }

        PreparedStatement prpStmt = connection.prepareStatement(command);
        prpStmt.setString(1, job_id);
        prpStmt.setInt(2, step_id);
        prpStmt.setString(3, step_name);
        prpStmt.setInt(4, status);
        prpStmt.setInt(5, numberSteps);
        prpStmt.setString(6, statusName);
        prpStmt.executeUpdate();
    }
}
