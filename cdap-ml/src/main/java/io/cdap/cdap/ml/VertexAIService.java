package io.cdap.cdap.ml;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import java.io.IOException;

public class VertexAIService {

  public String summarizePipeline(String project, String location, String modelName) throws IOException {
    VertexAI vertexAI = new VertexAI.Builder()
        .setLocation(location)
        .setProjectId(project)
        // .setCredentials(GoogleCredentials.getApplicationDefault())
        .build();

    GenerativeModel model = new GenerativeModel("gemini-1.5-pro", vertexAI);
    String inputText = "How many colors are in the rainbow?";
    GenerateContentResponse response = model.generateContent(inputText);
    String summary = ResponseHandler.getText(response);
    vertexAI.close();
    return "Hello World!" + "\n" + summary;
  }
}
