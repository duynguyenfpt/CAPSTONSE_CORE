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
        String partitionBy = "request_date";
        System.out.println(partitionBy.split(",")[0]);
    }
}
