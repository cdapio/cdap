package io.cdap.cdap.ml;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
// import com.google.cloud.vertexai.VertexAI;
// import com.google.cloud.vertexai.api.GenerateContentResponse;
// import com.google.cloud.vertexai.generativeai.GenerativeModel;
// import com.google.cloud.vertexai.generativeai.ResponseHandler;
import com.google.cloud.aiplatform.v1.Candidate;
import com.google.cloud.aiplatform.v1.Content;
import com.google.cloud.aiplatform.v1.EndpointName;
import com.google.cloud.aiplatform.v1.GenerateContentRequest;
import com.google.cloud.aiplatform.v1.GenerateContentResponse;
import com.google.cloud.aiplatform.v1.Part;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.protobuf.ByteString;
import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.io.IOException;
import org.apache.http.util.EntityUtils;
public class VertexAIService {
  public String summarizePipeline(String project,
      String location,
      String modelName,
      String accessTokenString) throws IOException {

    JsonObject mainObject = new JsonObject();

    // Create an empty JsonArray for contents
    JsonArray contentsArray = new JsonArray();

    // Create an object for a content item
    JsonObject contentObject = new JsonObject();
    contentObject.addProperty("role", "USER");

    // Create an empty JsonArray for parts
    JsonArray partsArray = new JsonArray();

    // Create an object for a part item
    JsonObject partObject = new JsonObject();
    partObject.addProperty("text", "Write Hanuman Chalisa");

    // Add the part object to the parts array
    partsArray.add(partObject);

    // Add the parts array to the content object
    contentObject.add("parts", partsArray);

    // Add the content object to the contents array
    contentsArray.add(contentObject);

    // Add the contents array to the main object
    mainObject.add("contents", contentsArray);

    // Convert the JsonObject to a JSON string
    String jsonPayload = mainObject.toString();

    // Create a CloseableHttpClient
    CloseableHttpClient httpClient = HttpClients.createDefault();

    // Specify the endpoint URL
    String endpointUrl = "https://us-west1-aiplatform.googleapis.com/v1/projects/" + project +
        "/locations/"+ location +"/publishers/google/models/"+ modelName + ":generateContent";

    // Create an HttpPost object with the endpoint URL
    HttpPost httpPost = new HttpPost(endpointUrl);

    // Set the JSON payload as the StringEntity of the HttpPost request
    StringEntity entity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON);
    httpPost.setEntity(entity);

    httpPost.setHeader("Content-Type", "application/json");
    String apiKey = accessTokenString;
    httpPost.setHeader("Authorization", "Bearer " + apiKey);
    // Execute the request and get the response
    CloseableHttpResponse response = httpClient.execute(httpPost);
    String responseBody="";
    String summary="";
    try {
      // Print the response status
      System.out.println("Response status: " + response.getStatusLine());

      // Print the response body, if needed
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        responseBody = EntityUtils.toString(responseEntity);
        System.out.println("Response body: " + responseBody);
        // Parse JSON response
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);

        // Extract 'candidates' array
        JsonArray candidates = jsonObject.getAsJsonArray("candidates");
        for (JsonElement candidateElement : candidates) {
          JsonObject candidateObject = candidateElement.getAsJsonObject();

          // Extract 'parts' array from 'content'
          JsonObject content = candidateObject.getAsJsonObject("content");
          JsonArray parts = content.getAsJsonArray("parts");

          // Extract 'text' from each 'part' and add to the list
          for (JsonElement partElement : parts) {
            JsonObject part = partElement.getAsJsonObject();
            String text = part.get("text").getAsString();
            summary=text;
          }
        }
      }
    } finally {
      // Close the response and HttpClient
      response.close();
      httpClient.close();
    }
    return summary;
  }

}
