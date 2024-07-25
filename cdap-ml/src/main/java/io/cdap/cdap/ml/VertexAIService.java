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
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class VertexAIService {

  // public String summarizePipeline(String project,
  //     String location,
  //     String modelName,
  //     String accessTokenString) throws IOException {
  //   VertexAI.Builder vertexAIBuilder = new VertexAI.Builder()
  //       .setLocation(location)
  //       .setProjectId(project);
  //
  //   if (!Strings.isNullOrEmpty(accessTokenString)) {
  //     AccessToken accessToken = new AccessToken(accessTokenString, null);
  //     GoogleCredentials credentials = GoogleCredentials.create(accessToken);
  //     vertexAIBuilder.setCredentials(credentials);
  //   }
  //
  //   VertexAI vertexAI = vertexAIBuilder.build();
  //   GenerativeModel model = new GenerativeModel(modelName, vertexAI);
  //   String inputText = "How many colors are in the rainbow?";
  //   GenerateContentResponse response = model.generateContent(inputText);
  //   String summary = ResponseHandler.getText(response);
  //   vertexAI.close();
  //   return "Hello World!" + "\n" + summary;
  // }

  // public String summarizePipeline(String project,
  //     String location,
  //     String modelName,
  //     String accessTokenString) throws IOException {
  //   EndpointName endpointName = EndpointName.ofProjectLocationPublisherModelName(project, location, "google", modelName);
  //   PredictionServiceClient predictionServiceClient = PredictionServiceClient.create(PredictionServiceSettings.newBuilder()
  //       .setEndpoint(endpointName.getEndpoint())
  //       .build());
  //   // 4. Prepare Content for Generation
  //   Content promptContent = Content.newBuilder()
  //       .setRole("USER")
  //       .setParts(0, Part.parseFrom(ByteString.copyFromUtf8("Write a creative story about a time-traveling cat.")))
  //       .build();
  //
  //   GenerateContentRequest request = GenerateContentRequest.newBuilder()
  //       .addContents(promptContent) // You can add multiple contents
  //       .build();
  //
  //   // 5. Call the GenerateContent API
  //   String summary = "";
  //   GenerateContentResponse response = predictionServiceClient.generateContent(request);
  //   for (Candidate candidate : response.getCandidatesList()) {
  //     for (Part part : candidate.getContent().getPartsList()) {
  //       summary += part.getText();
  //     }
  //   }
  //   return "Hello World!" + "\n" + summary;
  // }

  public String summarizePipeline(String project,
      String location,
      String modelName,
      String accessTokenString) throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    // TODO
    HttpPost request = new HttpPost("url");
    JsonObject payload = new JsonObject();
    
  }
}
