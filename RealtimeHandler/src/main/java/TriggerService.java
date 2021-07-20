import org.apache.spark.sql.SparkSession;

public class TriggerService {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Tracking Events")
                .getOrCreate();
        String bootstrapServer = "localhost:9092";
        String startingOffsets = "latest";
        sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
    }
}
