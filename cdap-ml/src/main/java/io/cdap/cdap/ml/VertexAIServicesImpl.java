package io.cdap.cdap.ml;

import com.google.cloud.aiplatform.v1.Content;
import com.google.cloud.aiplatform.v1.GenerateContentRequest;
import com.google.cloud.aiplatform.v1.Part;
import com.google.protobuf.util.JsonFormat;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.io.IOException;
import io.cdap.cdap.ml.spi.VertexAIServices;
public class VertexAIServicesImpl implements VertexAIServices{
  private CConfiguration configuration;
  public VertexAIServicesImpl(CConfiguration configuration){
    this.configuration = configuration;
  }
  public String summarizePipeline(String appArchitecture,
      String format) throws IOException {

    String prompt;
    if(format=="json"){
      String jsonPrompt = configuration.get(Constants.JSON_PROMPT);
      prompt= appArchitecture + "\n" + jsonPrompt;
    }
    else{
      String markDownPrompt= configuration.get(Constants.MARKDOWN_PROMPT);
      prompt= appArchitecture + "\n" + markDownPrompt;
    }
    GenerateContentRequest request = GenerateContentRequest.newBuilder().addContents(
        Content.newBuilder().addParts(Part.newBuilder().setText(prompt).build()).setRole("USER").build()).build();
    String jsonPayload = JsonFormat.printer().print(request);
    String summary = new VertexAIHttpClient(configuration).generateContent(jsonPayload);
    return summary;
  }
}