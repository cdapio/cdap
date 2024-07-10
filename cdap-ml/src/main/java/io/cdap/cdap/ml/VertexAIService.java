package io.cdap.cdap.ml;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import com.google.common.base.Strings;
import java.io.IOException;

public class VertexAIService {

  public String summarizePipeline(String project,
      String location,
      String modelName,
      String accessTokenString) throws IOException {
    VertexAI.Builder vertexAIBuilder = new VertexAI.Builder()
        .setLocation(location)
        .setProjectId(project);

    if (!Strings.isNullOrEmpty(accessTokenString)) {
      AccessToken accessToken = new AccessToken(accessTokenString, null);
      GoogleCredentials credentials = GoogleCredentials.create(accessToken);
      vertexAIBuilder.setCredentials(credentials);
    }

    VertexAI vertexAI = vertexAIBuilder.build();
    GenerativeModel model = new GenerativeModel(modelName, vertexAI);
    String inputText = "How many colors are in the rainbow?";
    GenerateContentResponse response = model.generateContent(inputText);
    String summary = ResponseHandler.getText(response);
    vertexAI.close();
    return "Hello World!" + "\n" + summary;
  }
}
