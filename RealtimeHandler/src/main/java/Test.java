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
        String schema = "id,fullname,birthdate,gender,national_info,province,district,wards,apartment_number,phone,social_insurance_number,identify_number,job,has_travel_to_other_area,has_symptoms,physical_contact,in_epidemic_area";
        ArrayList<String> listCols = new ArrayList<>(Arrays.asList(schema.split(",")));
        Collections.sort(listCols);
        for (String col : listCols) {
            System.out.println(col);
        }
    }
}
