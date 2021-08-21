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
        String schema = "\"has_travel_to_other_area:boolean,\"fullname:character,\"identify_number_card:character,\"wards:character,\"spo2_before:integer,\"apartment_number:character,\"vaccinate_type:character,\"birthdate:date,\"id:integer,\"nation:character,\"blood_pressure_before:character,\"phone_number:character,\"district:character,\"province:character,\"has_symptoms:boolean,\"email:character,\"blood_pressure_after:character,\"vaccinate_at:character,\"gender:character,\"physical_contact:character,\"has_symptoms_after:boolean,\"spo2_after:integer,\"has_background_disease:boolean,";
        schema = schema.toUpperCase();
        System.out.println(schema);
        ArrayList<String> mergeCols = new ArrayList<>(Arrays.asList("APARTMENT_NUMBER","BIRTHDATE","BLOOD_PRESSURE_AFTER","BLOOD_PRESSURE_BEFORE","DISTRICT","EMAIL","FULLNAME","GENDER","HAS_BACKGROUND_DISEASE","HAS_SYMPTOMS","HAS_SYMPTOMS_AFTER","HAS_TRAVEL_TO_OTHER_AREA","ID","IDENTIFY_NUMBER_CARD","MODIFIED","NATION","OPERATION","PHONE_NUMBER","PHYSICAL_CONTACT","PROVINCE","SPO2_AFTER","SPO2_BEFORE","VACCINATE_AT","VACCINATE_TYPE","WARDS"));
        ArrayList<String> newCDCCols = new ArrayList<>(Arrays.asList("APARTMENT_NUMBER","BIRTHDATE","BLOOD_PRESSURE_AFTER","BLOOD_PRESSURE_BEFORE","DISTRICT","EMAIL","FULLNAME","GENDER","HAS_BACKGROUND_DISEASE","HAS_SYMPTOMS","HAS_SYMPTOMS_AFTER","HAS_TRAVEL_TO_OTHER_AREA","ID","IDENTIFY_NUMBER_CARD","NATION","OPERATION","PHONE_NUMBER","PHYSICAL_CONTACT","PROVINCE","SPO2_AFTER","SPO2_BEFORE","VACCINATE_AT","VACCINATE_TYPE","WARDS"));
        ArrayList<String> newSourceCols = new ArrayList<>(Arrays.asList("APARTMENT_NUMBER","BIRTHDATE","BLOOD_PRESSURE_AFTER","BLOOD_PRESSURE_BEFORE","DISTRICT","EMAIL","FULLNAME","GENDER","HAS_BACKGROUND_DISEASE","HAS_SYMPTOMS","HAS_SYMPTOMS_AFTER","HAS_TRAVEL_TO_OTHER_AREA","ID","IDENTIFY_NUMBER_CARD","MODIFIED","NATION","PHONE_NUMBER","PHYSICAL_CONTACT","PROVINCE","SPO2_AFTER","SPO2_BEFORE","VACCINATE_AT","VACCINATE_TYPE","WARDS"));
        for (String col : mergeCols) {
            System.out.println("\""+col+":");
            int startIndex = schema.indexOf("\""+col+":") + col.length() + 2;
            int endIndex = schema.indexOf(",", startIndex );
            String type = schema.substring(startIndex, endIndex);
            col = col.toUpperCase();
            System.out.println(col);
            System.out.println(type);
            System.out.println("------------------");
        }
    }
}
