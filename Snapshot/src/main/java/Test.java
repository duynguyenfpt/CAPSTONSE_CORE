import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Test {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("fs.defaultFS", "hdfs://10.8.0.1:9000");
        FSDataInputStream in = null;
        //OutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            // Input file path
            Path inFile = new Path("/user/csv/test/part-00000-80c9c331-1ffc-4c2c-8f0c-4fe97d0f1124-c000.csv");
//            Path inFile = new Path(args[0]);

            // Check if file exists at the given location
            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                throw new IOException("Input file not found");
            }
            in = fs.open(inFile);
            IOUtils.copyBytes(in, System.out, 512, false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
