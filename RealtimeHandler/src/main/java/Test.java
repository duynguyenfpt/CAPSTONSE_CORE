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
        String schema = "[\"to_date:VARCHAR2\",\"date_vehicle:VARCHAR2\",\"vehicle:VARCHAR2\",\"vehicle_plate_number:VARCHAR2\",\"current_workplace:VARCHAR2\",\"t1_anaphylaxis:VARCHAR2\",\"t1_covid19:VARCHAR2\",\"t1_othervac_14days:VARCHAR2\",\"t1_chronic:VARCHAR2\",\"t1_is_immunodeficiency:VARCHAR2\",\"t1_acute_illness:VARCHAR2\",\"t1_is_pregnant_breastfeeding:VARCHAR2\",\"t1_is_allergies:VARCHAR2\",\"t1_underlying_disease:VARCHAR2\",\"t1_is_coagulation_disorder:VARCHAR2\",\"t1_is_older_65_age:VARCHAR2\",\"t1_body_temperature:VARCHAR2\",\"t1_pulse:NUMBER\",\"t1_blood_pressure:VARCHAR2\",\"t1_breathing:NUMBER\",\"t1_spo2:NUMBER\",\"t1_is_abnormal_heart_lungs:VARCHAR2\",\"t1_is_consciousness_disorder:VARCHAR2\",\"t1_is_vaccinated:VARCHAR2\",\"t1_vaccinate_type:VARCHAR2\",\"t1_eligible_vaccination:VARCHAR2\",\"t1_no_same_vaccine_type:VARCHAR2\",\"t1_delayed_vaccination:VARCHAR2\",\"t1_transfer_vaccination:VARCHAR2\",\"t1_transfer_at_hospital:VARCHAR2\",\"t1_transfer_reason:VARCHAR2\",\"t1_sign_at:DATE\",\"t1_doctor:VARCHAR2\",\"tmp2_agree_vaccinated:VARCHAR2\",\"tmp3_target:VARCHAR2\",\"tmp3_body_temperature_before:NUMBER\",\"tmp3_pulse_before:NUMBER\",\"tmp3_blood_pressure_before:VARCHAR2\",\"tmp3_spo2_before:NUMBER\",\"tmp3_other_symptoms_before:VARCHAR2\",\"tmp3_table_vaccinate:VARCHAR2\",\"tmp3_injection_vial_id:NUMBER\",\"tmp3_vaccinate_at:DATE\",\"tmp3_pulse_after_30m:NUMBER\",\"tmp3_blood_pressure_after_30m:VARCHAR2\",\"tmp3_spo2_after_30m:NUMBER\",\"tmp3_other_symptoms_after_30m:VARCHAR2\",\"tmp3_pulse_after_1h:NUMBER\",\"tmp3_blood_pressure__after_1h:VARCHAR2\",\"tmp3_spo2__after_1h:NUMBER\",\"other_symptoms_after_1h:VARCHAR2\",\"fpt_id:NUMBER\",\"name_employee:VARCHAR2\",\"identify_number_card:VARCHAR2\",\"gender:VARCHAR2\",\"birthdate:DATE\",\"department:VARCHAR2\",\"residence_province:VARCHAR2\",\"residence_district:VARCHAR2\",\"residence_wards:VARCHAR2\",\"residence_apartment_number:VARCHAR2\",\"phone:VARCHAR2\",\"relative_name:VARCHAR2\",\"relative_phone:VARCHAR2\",\"office_province:VARCHAR2\",\"office_district:VARCHAR2\",\"office_wards:VARCHAR2\",\"office_apartment_number:VARCHAR2\",\"has_symptoms:VARCHAR2\",\"physical_contact:VARCHAR2\",\"epidemic_area:VARCHAR2\",\"categorize_yourself:VARCHAR2\",\"date_travel:VARCHAR2\",\"province_travel:VARCHAR2\",\"district_travel:VARCHAR2\",\"wards_travel:VARCHAR2\",\"from_date:VARCHAR2\"]";
        Pattern pattern = Pattern.compile("\"([a-zA-Z1-9_:]+)");
        System.out.println(schema);
        System.out.println();
        Matcher matcher = pattern.matcher(schema);
        while (matcher.find()) {
            System.out.println(matcher.group(1));
        }
    }
}
