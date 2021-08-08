import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

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
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8280");
        String uri = "hdfs://127.0.0.1:9000/user/csv/test/part-00000-80c9c331-1ffc-4c2c-8f0c-4fe97d0f1124-c000.csv";
        InputStream in = null;
        //OutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            try {
                in = fs.open(new Path(uri));
                IOUtils.copyBytes(in, System.out, 512, false);
            } finally {
                IOUtils.closeStream(in);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
