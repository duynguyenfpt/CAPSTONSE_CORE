import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

import utils.*;

public class SparkWriter {
    public static void main(String[] args) throws SQLException {
        // Snapshot
        String jobID = "";
        //
        try {
            String database = args[0];
            String table = args[1];
            String username = args[2];
            String password = args[3];
            String host = args[4];
            String port = args[5];
            String partitionBy = args[6];
            jobID = args[7];
            SparkSession sparkSession = SparkSession
                    .builder()
                    .appName(String.format("connect database %s and table %s to parquet", database, table))
                    .getOrCreate();

            String path = String.format("/user/test/%s/%s/", database, table);
            String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", host, port, database);
            String currentTime = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            Dataset<Row> input = sparkSession.read()
                    .format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver")
                    .option("dbtable", table).option("user", username).option("password", password)
                    .load()
                    .withColumn("modified", lit(currentTime));

            input.write()
                    .mode("overwrite")
                    .partitionBy(partitionBy)
                    .parquet(path);
            sqlUtils.insertJobLog(jobID, 2, "snapshot_data", 1);
        } catch (Exception exception) {
            sqlUtils.insertJobLog(jobID, 2, "snapshot_data", 0);
            System.out.println(exception.getMessage());
        }
    }
}
