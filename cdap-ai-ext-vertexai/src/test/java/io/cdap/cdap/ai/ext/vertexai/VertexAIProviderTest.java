package io.cdap.cdap.ai.ext.vertexai;

import com.google.cloud.vertexai.api.Candidate;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.ContentMaker;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.proto.artifact.AppRequest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link VertexAIProvider}.
 */
public class VertexAIProviderTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final String TEST_LOCATION = "test-location";
  private static final String TEST_MODEL_NAME = "test-model";
  private static final String SUMMARY_PROMPT = "Summary prompt";
  private static VertexAIConfiguration conf;
  private static GenerativeModel mockModel;
  private static VertexAIProvider provider;

  @BeforeClass
  public static void setUp() {
    Map<String, String> properties = new HashMap<>();
    properties.put(VertexAIConfiguration.PROJECT_ID, TEST_PROJECT_ID);
    properties.put(VertexAIConfiguration.LOCATION_NAME, TEST_LOCATION);
    properties.put(VertexAIConfiguration.MODEL_NAME, TEST_MODEL_NAME);
    properties.put(VertexAIConfiguration.Prompts.PIPELINE_MARKDOWN_SUMMARY, SUMMARY_PROMPT);
    conf = VertexAIConfiguration.create(properties);
    mockModel = Mockito.mock(GenerativeModel.class);
    provider = new VertexAIProvider(conf, mockModel);
  }

  @Test(expected = NullPointerException.class)
  public void testNullAppRequest() throws IOException {
    provider.summarizeApp(null, "markdown");
  }

  @Test(expected = IOException.class)
  public void testGenerateContentThrowsIOException() throws IOException {
    ArtifactSummary artifactSummary = new ArtifactSummary("pipeline", "1.0.0");
    AppRequest appRequest = new AppRequest<>(artifactSummary);
    when(mockModel.generateContent(ArgumentMatchers.<Content>any()))
            .thenThrow(new IOException("Simulated IO Exception"));
    provider.summarizeApp(appRequest, "markdown");
  }

  @Test
  public void testSummarizeApp() throws IOException {
    ArtifactSummary artifactSummary = new ArtifactSummary("pipeline", "1.0.0");
    AppRequest appRequest = new AppRequest<>(artifactSummary);
    GenerateContentResponse response = GenerateContentResponse.newBuilder()
            .addCandidates(Candidate.newBuilder()
                    .setContent(ContentMaker.fromString("dummy result"))
                    .setFinishReason(Candidate.FinishReason.STOP)
                    .build())
            .build();
    when(mockModel.generateContent(ArgumentMatchers.<Content>any()))
            .thenReturn(response);
    String summary = provider.summarizeApp(appRequest, "markdown");
    Assert.assertEquals("dummy result", summary);
  }
}
