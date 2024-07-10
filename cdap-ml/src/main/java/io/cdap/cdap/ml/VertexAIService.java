package io.cdap.cdap.ml;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.PredictionServiceClient;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import java.io.FileInputStream;
import java.io.IOException;

public class VertexAIService {

  public String summarizePipeline(String project, String location, String modelName, String accessTokenString) throws IOException {
    AccessToken accessToken = new AccessToken(accessTokenString, null);
    GoogleCredentials credentials = GoogleCredentials.create(accessToken);
    VertexAI vertexAI = new VertexAI.Builder()
        .setLocation(location)
        .setProjectId(project)
        .setCredentials(credentials)
        .build();

    GenerativeModel model = new GenerativeModel(modelName, vertexAI);
    String inputText = "How many colors are in the rainbow?";
    GenerateContentResponse response = model.generateContent(inputText);
    String summary = ResponseHandler.getText(response);
    vertexAI.close();
    return "Hello World!" + "\n" + summary;
  }
}
