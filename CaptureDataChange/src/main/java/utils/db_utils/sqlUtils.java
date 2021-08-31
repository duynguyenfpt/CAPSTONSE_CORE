package utils.db_utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import com.google.gson.Gson;
import models.CDCModel;
import models.LogModel;
import models.MergeRequestModel;
import models.OffsetModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

public class sqlUtils {
    public static String getConnectionString(String host, String port, String db, String userName, String password) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", host, port, db, userName, password);
    }

    public static String getConnectionString(String host, String port, String db, String userName, String password, String databaseType) {
//        return String.format("jdbc:mysql://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&verifyServerCertificate=false&autoReconnect=true", host, port, db, userName, password);
        return String.format("jdbc:%s://%s:%s/%s?user=%s&password=%s&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true", databaseType, host, port, db, userName, password);
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

    public static void mergeRequestProducer(String kafkaCluster, String kafkaTopic, MergeRequestModel rmr) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduce the no of requests less than 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        Gson gson = new Gson();
        producer.send(new ProducerRecord<String, String>(kafkaTopic, rmr.getMergeTable(), gson.toJson(rmr)));
        producer.close();
    }

    public static Connection getConnectionOracle(String username, String password, String host, String port, String SID) {
        Connection conn = null;
        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:%s/%s@%s:%s:%s", username, password, host, port, SID));
//            conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, SID), username, password);
            System.out.println("connected successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return conn;
    }

    public static ArrayList<String> getListFieldsPostgresql(Connection connection, String tableName) {
        ArrayList<String> results = new ArrayList<String>();
        String describeQuery = "" +
                "SELECT *\n" +
                "  FROM information_schema.columns\n" +
                " WHERE table_schema = ? \n" +
                "   AND table_name   = ?;";
        try {
            PreparedStatement statement = connection.prepareStatement(describeQuery);
            statement.setString(1, tableName.split("[.]")[0]);
            statement.setString(2, tableName.split("[.]")[1]);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                results.add(rs.getString("column_name").toLowerCase() + ":" + rs.getString("data_type"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return results;
    }

    public static ArrayList<String> getListFields(Connection connection, String tableName) {
        ArrayList<String> results = new ArrayList<String>();
        String describeQuery = "DESCRIBE " + tableName;
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(describeQuery);
            while (rs.next()) {
                results.add(rs.getString("Field").toLowerCase() + ":" + rs.getString("Type"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return results;
    }

    public static ArrayList<String> getListFieldsOracle(Connection connection, String tableName) {
        ArrayList<String> results = new ArrayList<String>();
        String describeQuery = "" +
                "SELECT table_name, column_name, data_type, data_length\n" +
                "FROM USER_TAB_COLUMNS\n" +
                "WHERE table_name = ? ";
        try {
            PreparedStatement statement = connection.prepareStatement(describeQuery);
            statement.setString(1, tableName.toUpperCase());
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                results.add(rs.getString("COLUMN_NAME").toLowerCase() + ":" + rs.getString("DATA_TYPE"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return results;
    }


    public static String createTrigger(Connection connection, String host, String port, String database,
                                       String table, int operationType, String cdcDB, String cdcTable) {
        // set operation
        String operation = "INSERT";
        if (operationType == 2) {
            operation = "UPDATE";
        } else if (operationType == 3) {
            operation = "DELETE";
        }
        //
        //
        String triggerTemplate = "" +
                "CREATE TRIGGER test_after_%s_%s_%s\n" +
                "    AFTER %s ON %s.%s\n" +
                "    FOR EACH ROW\n" +
                " INSERT INTO %s.%s\n" +
                " SET\n" +
                "  `database_url` = '%s',\n" +
                "  `database_port` = '%s',\n" +
                "  `database_name` = '%s',\n" +
                "  `table_name` = '%s',\n" +
                "  `operation` =  %d,\n" +
                "  `value` = %s,\n" +
                "  `schema` = '%s'";
        //
        String valueDetail = "";
        // convert fields to json
        ArrayList<String> fields = getListFields(connection, table);
        Gson gson = new Gson();
        valueDetail = "concat('{',";
        //
        int count = 1;
        for (String field_value : fields) {
            String field = field_value.substring(0, field_value.indexOf(":"));
            if (operationType != 3) {
                valueDetail += String.format("'\"%s\":','\"',new.%s,'\"'", field, field);
            } else {
                valueDetail += String.format("'\"%s\":','\"',old.%s,'\"'", field, field);
            }
            if (count < fields.size()) {
                valueDetail += ",',',";
            } else {
                valueDetail += ",'}')";
            }
            count++;
        }
        ;
        return String.format(triggerTemplate, database, table, operation, operation,
                database, table, cdcDB, cdcTable, host, port, database, table, operationType, valueDetail, gson.toJson(fields));
    }

    public static String createTriggerOracle(Connection connection, String host, String port, String database,
                                             String table, int operationType, String cdcDB, String owner) {
        // set operation
        String operation = "INSERT";
        if (operationType == 2) {
            operation = "UPDATE";
        } else if (operationType == 3) {
            operation = "DELETE";
        }
        //
        //
        String triggerTemplate = "" +
                "CREATE OR REPLACE TRIGGER %s_before_%s\n" +
                "AFTER %s\n" +
                "   ON %s\n" +
                "   FOR EACH ROW\n" +
                "BEGIN \n" +
                "   INSERT INTO %s.%s(database_url,database_port,database_name,table_name,schema,value,operation)" +
                "VALUES('%s','%s','%s','%s','%s',%s,'%d');\n " +
                "END;";
        //
        String valueDetail = "";
        // convert fields to json
        ArrayList<String> fields = getListFieldsOracle(connection, table);
        Gson gson = new Gson();
        valueDetail = "'{";
        //
        int count = 1;
        for (String field_value : fields) {
            String field = field_value.substring(0, field_value.indexOf(":"));
            if (operationType != 3) {
                valueDetail += String.format("\"%s\":\"' || :NEW.%s", field, field);
            } else {
                valueDetail += String.format("\"%s\":\"' || :OLD.%s", field, field);
            }
            if (count < fields.size()) {
                valueDetail += "|| '\",";
            } else {
                valueDetail += "|| '\"}'";
            }
            count++;
        }
        ;
        return String.format(triggerTemplate, table, operation, operation, table, owner, cdcDB, host,
                port, database, table, gson.toJson(fields), valueDetail, operationType);
    }

    public static String createSchemaFunctionPostgres() {
        return "create or replace function create_cdc_schema(schemaName text,tableName text)\n" +
                "returns text as\n" +
                "$$\n" +
                "declare schemaValue text := '';\n" +
                "begin \n" +
                "\tselect string_agg('\"' || column_name || ':' || split_part(data_type,' ',1) || '\"' ,',') into schemaValue\n" +
                "\tfrom information_schema.columns\n" +
                "\twhere table_schema = schemaName and table_name = tableName;\n" +
                "\treturn '[' || schemaValue || ']';\n" +
                "end;\n" +
                "$$ LANGUAGE plpgsql;";
    }


    public static String createTriggerFunctionPostgresql(Connection connection, String host, String port, String database,
                                                         String table, int operationType, String cdcDB, String cdcTable, String username, String password) {
        // set operation
        String operation = "INSERT";
        if (operationType == 2) {
            operation = "UPDATE";
        } else if (operationType == 3) {
            operation = "DELETE";
        }
        //
        String schema = table.split("[.]")[0];
        String tableName = table.split("[.]")[1];
        //
        String triggerTemplate = "create or replace function %s_%s_%s_cdc_4912929()\n" +
                "returns trigger as \n" +
                "$$\n" +
                "declare \n" +
                "\tdatabase_host varchar(20) := '%s';\n" +
                "\tdatabase_port varchar(10) := '%s';\n" +
                "\tdatabase_name varchar(10) := '%s';\n" +
                "    schemaName varchar(20) := '%s';\n" +
                "\ttableName varchar(100) := '%s';\n" +
                "\trecordValue text := E'{';\n" +
                "\toperation int := %d;\n" +
                "begin \n" +
                "\tperform dblink_connect('connection','hostaddr=%s port=%s dbname=cdc_4912929__cdc user=%s password=%s');\n" +
                "\tperform dblink_exec('connection','BEGIN');\n" +
                "\tif operation < 3 then\n" +
                "\t\tperform dblink_exec('connection',\n" +
                "\t\tFORMAT(E'INSERT INTO public.cdc_detail(database_url,database_port,database_name,table_name,schema,value,operation) \n" +
                "\t\t \tVALUES(\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\')',database_host,database_port,database_name,(schemaName || '.' || tableName ),\n" +
                "\t\t create_cdc_schema(schemaName,tableName),row_to_json(new),operation));\n" +
                "\telse \n" +
                "\t\tperform dblink_exec('connection',\n" +
                "\t\tFORMAT(E'INSERT INTO public.cdc_detail(database_url,database_port,database_name,table_name,schema,value,operation) \n" +
                "\t\t \tVALUES(\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\',\\'%%s\\')',database_host,database_port,database_name,(schemaName || '.' || tableName ),\n" +
                "\t\t create_cdc_schema(schemaName,tableName),row_to_json(old),operation));\n" +
                "\tend if;\n" +
                "\tperform dblink_exec('connection','COMMIT');\n" +
                "\tperform dblink_disconnect('connection');\n" +
                "\tRETURN NEW;\n" +
                "end;\n" +
                "$$ language plpgsql;";
        ;
        return String.format(triggerTemplate, operation, schema, tableName, host, port, database, schema, tableName, operationType, host, port, username, password);
    }

    public static OffsetModel getOffsets(Connection connection, String host, String port) {
        String query = "SELECT offsets, dbtype from cdc.offsets where " +
                "`database_host` = ? and `database_port` = ?";
        System.out.println(host);
        System.out.println(port);
        PreparedStatement prpStmt = null;
        OffsetModel result = new OffsetModel();
        try {
            prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                result.setOffsets(rs.getInt("offsets"));
                result.setDbType(rs.getString("dbtype"));
                return result;
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public static Integer getLatestID(Connection connection) {
        String query = "SELECT max(id) as max_id from cdc_detail";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                return rs.getInt("max_id");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public static Integer getLatestPostgresql(String host, String port, String username, String password) throws SQLException {
        Connection connection = sqlUtils.getConnection(sqlUtils.getConnectionString(host, port, "cdc_4912929__cdc", username, password, "postgresql"));
        String query = "SELECT max(id) as max_id from public.cdc_detail";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                return rs.getInt("max_id");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            connection.close();
        }
        return null;
    }

    public static Integer getLatestOracle(Connection sourceConnection) throws SQLException {
        String queryCheckDB = "SELECT\n" +
                "  table_name, owner\n" +
                "FROM\n" +
                "  all_tables\n" +
                "WHERE upper(table_name)='CDC_4912929__CDC'\n" +
                "ORDER BY\n" +
                "  owner, table_name";
        try {
            //
            Statement stmt = sourceConnection.createStatement();
            ResultSet rs2 = stmt.executeQuery(queryCheckDB);
            String owner = "";
            if (rs2.next()) {
                owner = rs2.getString("owner");
            }
            //
            String query = String.format("SELECT max(id) as max_id from %s.cdc_4912929__cdc", owner);
            PreparedStatement prpStmt = sourceConnection.prepareStatement(query);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                return rs.getInt("max_id");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    public static ArrayList<CDCModel> getCDCs(Connection connection, int offsets, int max_id) {
        ArrayList<CDCModel> listCDCs = new ArrayList<CDCModel>();
        String getDataQuery = "SELECT * FROM cdc_detail " +
                "where id > ? and id <= ? " +
                "order by table_name ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(getDataQuery);
            prpStmt.setInt(1, offsets);
            prpStmt.setInt(2, max_id);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                listCDCs.add(
                        new CDCModel(rs.getInt("id"), rs.getString("database_url"), rs.getString("database_port"),
                                rs.getString("database_name"), rs.getString("table_name"), rs.getString("schema"),
                                rs.getString("value"), rs.getInt("operation"), rs.getTimestamp("created").getTime() / 1000)
                );
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return listCDCs;
    }

    public static ArrayList<CDCModel> getCDCsOracle(Connection connection, int offsets, int max_id) {
        String queryCheckDB = "SELECT\n" +
                "  table_name, owner\n" +
                "FROM\n" +
                "  all_tables\n" +
                "WHERE upper(table_name)='CDC_4912929__CDC'\n" +
                "ORDER BY\n" +
                "  owner, table_name";

        ArrayList<CDCModel> listCDCs = new ArrayList<CDCModel>();
        String getDataQuery = "SELECT * FROM %s.CDC_4912929__CDC " +
                "where id > ? and id <= ? " +
                "order by table_name ";
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs2 = stmt.executeQuery(queryCheckDB);
            String owner = "";
            if (rs2.next()) {
                owner = rs2.getString("owner");
            }
            //
            PreparedStatement prpStmt = connection.prepareStatement(String.format(getDataQuery, owner));
            prpStmt.setInt(1, offsets);
            prpStmt.setInt(2, max_id);
            ResultSet rs = prpStmt.executeQuery();
            while (rs.next()) {
                listCDCs.add(
                        new CDCModel(rs.getInt("id"), rs.getString("database_url"), rs.getString("database_port"),
                                rs.getString("database_name"), rs.getString("table_name"), rs.getString("schema"),
                                rs.getString("value"), rs.getInt("operation"), rs.getTimestamp("created").getTime() / 1000)
                );
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return listCDCs;
    }

    public static void updateOffset(Connection connection, String host, String port, int offsets) {
        String query = "update cdc.offsets set offsets = ? where `database_host` = ? and `database_port` = ? ";
        try {
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setInt(1, offsets);
            prpStmt.setString(2, host);
            prpStmt.setString(3, port);
            prpStmt.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    public static void initOffset(Connection connection, String host, String port, String dbType) throws SQLException {
        // must check exist ?
        // if not then update rather than insert
        System.out.println("init offset");
        //
        String queryCheck = "select * from cdc.`offsets` where database_host = ? and database_port = ? ";
        PreparedStatement prpCheck = connection.prepareStatement(queryCheck);
        prpCheck.setString(1, host);
        prpCheck.setString(2, port);
        ResultSet rs = prpCheck.executeQuery();
        if (!rs.next()) {
            //
            String query = "insert into " +
                    "cdc.`offsets`(`database_host`,`database_port`,`offsets`, `dbtype`) values (?,?,?,?)";
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            prpStmt.setInt(3, 0);
            prpStmt.setString(4, dbType);
            prpStmt.executeUpdate();
        }
    }

    public static void initOffset(Connection sourceConnection, Connection connection,
                                  String host, String port, String dbType, String owner, String username) throws SQLException {
        // must check exist ?
        // if not then update rather than insert
        System.out.println("init offset");
        //
        String queryCheck = "select * from cdc.`offsets` where database_host = ? and database_port = ? and owner = ? ";
        PreparedStatement prpCheck = connection.prepareStatement(queryCheck);
        prpCheck.setString(1, host);
        prpCheck.setString(2, port);
        prpCheck.setString(3, username);
        ResultSet rs = prpCheck.executeQuery();

        // get latest offset
        int maxID = 0;
        Statement stmt = sourceConnection.createStatement();
        ResultSet rs2 = stmt.executeQuery(String.format("SELECT MAX(ID) as max_id FROM %s.cdc_4912929__cdc", owner));
        if (rs2.next()) {
            maxID = rs2.getInt("max_id");
        }
        //
        if (!rs.next()) {
            //
            String query = "insert into " +
                    "cdc.`offsets`(`database_host`,`database_port`,`offsets`, `dbtype`,`owner`) values (?,?,?,?,?)";
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setString(1, host);
            prpStmt.setString(2, port);
            prpStmt.setInt(3, maxID);
            prpStmt.setString(4, dbType);
            prpStmt.setString(5, username);
            prpStmt.executeUpdate();
        } else {
            String query = "update " +
                    "cdc.`offsets` set offset = ? where `database_host` = ? and `database_port` = ? and `owner` = ? ";
            PreparedStatement prpStmt = connection.prepareStatement(query);
            prpStmt.setInt(1, maxID);
            prpStmt.setString(2, host);
            prpStmt.setString(3, port);
            prpStmt.setString(4, owner);
            prpStmt.executeUpdate();
        }
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

    public static void getTableInfo(String host, String port, String database, String tableName) {

    }

    public static void updateReady(String host, String port, String database
            , String tableName, Connection connection, int readiness) throws SQLException {
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_ready = b'%d' " +
                "WHERE host = ? and port = ? and `database` = ? and `table` = ?", readiness);
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);
        prpStmt.executeUpdate();
    }

    public static void updateActive(String host, String port, String database
            , String tableName, Connection connection) throws SQLException {
        String updateQuery = String.format("UPDATE cdc.table_monitor SET is_active = b'0' " +
                ",is_ready = b'0',latest_offset = 0 " +
                "WHERE host = ? and port = ? and `database` = ? and `table` = ?");
        PreparedStatement prpStmt = connection.prepareStatement(updateQuery);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, tableName);
        prpStmt.executeUpdate();
    }

    public static void resetMonitor(String host, String port, String database,
                                    String table, Connection connection) throws SQLException {
        String queryCheck = "SELECT * FROM cdc.table_monitor Where `host` = ? and `port` = ? and `database` = ? and `table` = ?";
        PreparedStatement prpStmt = connection.prepareStatement(queryCheck);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        prpStmt.setString(3, database);
        prpStmt.setString(4, table);
        ResultSet rs = prpStmt.executeQuery();
        if (rs.next()) {
            updateActive(host, port, database, table, connection);
        } else {
            System.out.println("yoyoyo");
            String insetQuery = "insert into " +
                    "cdc.`table_monitor`(`host`,`port`,`database`,`table`, `is_active`,`is_ready`, `latest_offset`) values (?,?,?,?,b'0',b'0',?)";
            PreparedStatement insertStmt = connection.prepareStatement(insetQuery);
            insertStmt.setString(1, host);
            insertStmt.setString(2, port);
            insertStmt.setString(3, database);
            insertStmt.setString(4, table);
            insertStmt.setInt(5, 0);
            insertStmt.executeUpdate();
        }
    }

    public static void logProducer(String kafkaCluster, String kafkaTopic, LogModel log) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduce the no of requests less than 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        Gson gson = new Gson();
        producer.send(new ProducerRecord<String, String>(kafkaTopic, log.getHost() + "-" + log.getPort() + "-"
                + log.getDatabase_name() + "-" + log.getTable_name(), gson.toJson(log)));
        producer.close();
    }

    public static String getSID(String host, String port, Connection connection) throws SQLException {
        String query = "SELECT sid from webservice_test.database_infos di\n" +
                "inner join webservice_test.server_infos si\n" +
                "on si.deleted = 0 and di.deleted = 0 and di.server_info_id = si.id\n" +
                "where si.server_host = ? and port = ? ;";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("sid");
        }
        return null;
    }

    public static String getDBType(String host, String port, Connection connection) throws SQLException {
        String query = "SELECT database_type from webservice_test.database_infos di\n" +
                "inner join webservice_test.server_infos si\n" +
                "on si.deleted = 0 and di.deleted = 0 and di.server_info_id = si.id\n" +
                "where si.server_host = ? and port = ? ;";
        PreparedStatement prpStmt = connection.prepareStatement(query);
        prpStmt.setString(1, host);
        prpStmt.setString(2, port);
        ResultSet rs = prpStmt.executeQuery();
        while (rs.next()) {
            return rs.getString("database_type");
        }
        return null;
    }
}
