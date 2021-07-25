import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {
        try {
            throw new Exception("testing message");
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
    }
}
