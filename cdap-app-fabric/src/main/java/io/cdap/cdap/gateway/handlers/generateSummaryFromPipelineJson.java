package io.cdap.cdap.gateway.handlers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.net.URL;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
public class generateSummaryFromPipelineJson {
  public String generateSummaryFromPipelineJsonfunction(String pipelineJson) throws IOException {
    // Use VertexAI and GenerativeModel (assuming these are initialized elsewhere)
    VertexAI vertexAI = new VertexAI("vsethi-project2", "us-west1");
    GenerativeModel model = new GenerativeModel("gemini-1.5-pro", vertexAI);

    List<String> prompts = new ArrayList<>();
    prompts.add("Summarize the Cloud Data Fusion pipeline in HTML format"); // Add your prompts here
    //  prompts.add("As a data analyst I want to understand the Cloud Data Fusion pipeline. Summarize the pipeline for me");
//        String pr1 = """
//Summarize the following Cloud Data Fusion pipeline JSON and provide the output in JSON format with the following schema:
//
//{
//  "summary": "...", (A brief description of the pipeline)
//  "stages": [  ...(Array of objects representing pipeline stages)
//    {
//      "name": "...", (Name of the stage)
//      "type": "source" | "sink" | "transformation", (Type of the stage)
//      "plugin_configuration": {...} (Configuration specific to the plugin used in the stage)
//    },
//    ...
//  ],
//  "execution_configuration": { ... }, (Configuration for running the pipeline)
//    "resources": ..., (Resource requirements for the pipeline)
//    "connections": ..., (Connections used by the pipeline)
//    "schedule": ..., (Schedule for pipeline execution)
//    "engine": ..., (Engine used to execute the pipeline)
//    "concurrent_run": ..., (Flag for allowing concurrent runs)
//  },
//  "additional_configuration": { ... }, (Optional additional configurations)
//    "cdf_version": ..., (Cloud Data Fusion version used)
//    "logging": ..., (Logging configuration)
//    "preview_records": ... (Flag for enabling preview records)
//}
//
//**Please include all the above-mentioned properties in the output JSON, if they are present in the original pipeline JSON.**
//
//""" ;
//        String pr2 = """
//Summarize the following Cloud Data Fusion pipeline JSON and provide the output in JSON format with the following schema:
//
//{
//  "summary": "...", (A brief description of the pipeline)
//  "stages": [  ...(Array of objects representing pipeline stages)
//    {
//      "name": "...", (Name of the stage)
//      "version": "...", (version of plugin)
//      "type": "source" | "sink" | "transformation", (Type of the stage)
//      "plugin_configuration": {...} (Configuration specific to the plugin used in the stage and it must not include schema of plugin because it makes summary unreadable and directive of transformation plugin should be in simple plain english)
//    },
//    ...
//  ],
//  "execution_configuration": { ... }, (Configuration for running the pipeline)
//    "resources": ..., (Resource requirements for the pipeline)
//    "connections": ..., (Connections used by the pipeline)
//    "schedule": ..., (Schedule for pipeline execution)
//    "engine": ..., (Engine used to execute the pipeline)
//    "concurrent_run": ..., (Flag for allowing concurrent runs)
//  },
//  "additional_configuration": { ... }, (Optional additional configurations)
//    "cdf_version": ..., (Cloud Data Fusion version used)
//    "logging": ..., (Logging configuration)
//    "preview_records": ... (Flag for enabling preview records)
//}
//
//**Please include all the above-mentioned properties in the output JSON, if they are present in the original pipeline JSON.**
//
//""" ;
    //  prompts.add(pr1);
    //   prompts.add(pr2);

    String summary = "";
    for (String prompt : prompts) {
      String inputText = prompt + "\n" + pipelineJson;
      GenerateContentResponse respons = null;
      try{
        respons = model.generateContent(inputText);
        // Exit the loop after getting the first successful summary
      }
      catch (IOException e){
        // Handle exception and potentially log or retry
      }
      summary = ResponseHandler.getText(respons);
    }

    vertexAI.close();
    return summary;
  }
}
