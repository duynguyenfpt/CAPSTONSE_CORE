import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {
        String schema = "[\"request_id:int\",\"request_time:int\",\"request_date:int\",\"trans_amount:int\"]";
        Pattern pattern = Pattern.compile("\"([a-z_:]+)");
        Matcher matcher = pattern.matcher(schema);
        while (matcher.find()){
            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
        }
    }
}
