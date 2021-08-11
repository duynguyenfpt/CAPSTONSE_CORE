package sub_service;

import utils.db_utils.sqlUtils;

import java.sql.Connection;

public class ConnectionSingleton {
    // static variable single_instance of type Singleton
    private static ConnectionSingleton single_instance = null;

    // variable of type String
    public Connection connection;

    // private constructor restricted to this class itself
    private ConnectionSingleton() {
        connection = sqlUtils.getConnection(sqlUtils.getConnectionString("localhost", "3306",
                "cdc", "duynt", "Capstone123@"));
    }

    // static method to create instance of Singleton class
    public static ConnectionSingleton getInstance() {
        if (single_instance == null)
            single_instance = new ConnectionSingleton();

        return single_instance;
    }
}
