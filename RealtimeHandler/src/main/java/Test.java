import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Test {
    public static void main(String[] args) {
        String query = "select * from :merge_request.covid_2: where identity_number = '7153 4767 8382 1942'";
        Pattern pattern = Pattern.compile(":[a-zA-Z1-9._]+:");
        Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {
            String queryAlias = matcher.group(0).toLowerCase();
            if (!queryAlias.startsWith(":merge_request")) {
            } else {
                int firstDot = queryAlias.indexOf(".");
                System.out.println(firstDot);
                String mergeTable = queryAlias.substring(firstDot + 1, queryAlias.length() - 1);
                String mergePath = String.format("/user/merge_request/%s", mergeTable);
                System.out.println("merge table is: " + mergeTable);
                System.out.println("merge path is: " + mergePath);
                query = query.replaceAll(queryAlias, mergeTable);
                System.out.println(query);
            }
        }
        System.out.println();
    }
}
