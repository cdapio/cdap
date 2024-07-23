package io.cdap.cdap.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class VertexAIHttpClient {
    private HttpClient httpClient;
    private CConfiguration configuration;
    public VertexAIHttpClient(CConfiguration conf){
        this.configuration = conf;
        httpClient = HttpClients.createDefault();
    }
    public String generateContent(String jsonPayload) throws IOException {

      String project = configuration.get(Constants.PROJECT_NAME);
      String location = configuration.get(Constants.LOCATION_NAME);
      String modelName = configuration.get(Constants.MODEL_NAME);
      String endpointUrl = "https://us-west1-aiplatform.googleapis.com/v1/projects/" + project +
                           "/locations/"+ location +"/publishers/google/models/"+ modelName + ":generateContent";
      HttpPost httpPost = new HttpPost(endpointUrl);
      StringEntity entity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON);
      httpPost.setEntity(entity);
      httpPost.setHeader("Content-Type", "application/json");
      String accessToken = "";   // Generation of Access Token need to be implemented.
      httpPost.setHeader("Authorization", "Bearer " + accessToken);
      HttpResponse response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String responseBody = EntityUtils.toString(responseEntity);
      Gson gson = new Gson();
      JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
      String jsonResponse=jsonObject.toString();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(jsonResponse);
      String summary= rootNode.get("candidates").get(0).get("content").get("parts").get(0).get("text").asText();
      return summary;
  }
}
