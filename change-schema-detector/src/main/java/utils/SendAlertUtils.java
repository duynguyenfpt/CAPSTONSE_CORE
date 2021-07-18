package utils;

import models.Alert.AlertModel;
import models.DDLModel;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class SendAlertUtils {
    public static String sendPOST(String url, AlertModel model) throws IOException {
        String result = "";
        HttpPost post = new HttpPost(url);
        post.addHeader("content-type", "application/json");
        Gson gson = new Gson();
        post.setEntity(new StringEntity(gson.toJson(model)));
        System.out.println(gson.toJson(model));
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {
            result = EntityUtils.toString(response.getEntity());
        }
        return result;
    }
}
