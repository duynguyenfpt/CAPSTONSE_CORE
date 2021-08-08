import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Test {
    public static void main(String[] args) {
        //
        String query = "" +
                "SELECT * FROM :sale.product.employee: a " +
                "INNER JOIN :user.shop: b " +
                "ON a.shop_id = b.id;";
        Pattern pattern = Pattern.compile(":[a-zA-Z1-9.]+:");
        Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {
            System.out.println(matcher.group(0));
            String data = matcher.group(0);
            // remove : character
            data = data.replace(":", "");
            //
            int firstDotPos = data.indexOf(".");
            String alias = data.substring(0, firstDotPos);
            String tableName = data.substring(firstDotPos + 1);
            System.out.println(alias);
            System.out.println(tableName);
        }

    }
}
