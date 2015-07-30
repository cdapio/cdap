/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Workflow for Wikipedia data pipeline
 */
public class WikipediaPipelineWorkflow extends AbstractWorkflow {

  public static final String NAME = WikipediaPipelineWorkflow.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A workflow that demonstrates a typical data pipeline to process Wikipedia data.");
    addMapReduce(WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME);
    condition(new EnoughDataToProceed())
      .condition(new IsWikipediaSourceOnline())
        .addAction(new DownloadWikiDataAction())
      .otherwise()
        .addMapReduce(WikipediaPipelineApp.WIKIPEDIA_TO_DATASET_MR_NAME)
      .end()
      .addMapReduce(WikiContentValidatorAndNormalizer.NAME)
      .fork()
        .addSpark(SparkWikipediaAnalyzer.NAME)
      .also()
        .addMapReduce(TopNMapReduce.NAME)
      .join()
    .otherwise()
    .end();
  }

  static class EnoughDataToProceed implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(WorkflowContext context) {
      Map<String, String> runtimeArguments = context.getRuntimeArguments();
      int threshold = 10;
      if (runtimeArguments.containsKey("min.page.threshold")) {
        threshold = Integer.parseInt(runtimeArguments.get("min.page.threshold"));
      }
      WorkflowToken token = context.getToken();
      Value result = token.get("result", WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME);
      Value numPages = token.get("custom.num.records", WorkflowToken.Scope.SYSTEM);
      boolean conditionResult = result != null && result.getAsBoolean() &&
        numPages != null && numPages.getAsLong() > threshold;
      token.put("result", Value.of(conditionResult));

      // Also add information in the token on whether to download wikipedia data over the internet
      // NOTE: The following predicate can even consume this information through runtimeArguments. However,
      // we want to demonstrate the usage of workflow token through this example, so adding this indirection.
      if (runtimeArguments.containsKey("mode")) {
        token.put("online", Value.of(runtimeArguments.get("mode").equalsIgnoreCase("online")));
      }
      return conditionResult;
    }
  }

  static class IsWikipediaSourceOnline implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(WorkflowContext context) {
      WorkflowToken token = context.getToken();
      Value online = token.get("online", EnoughDataToProceed.class.getSimpleName(), WorkflowToken.Scope.USER);
      return online != null && online.getAsBoolean();
    }
  }

  private static class DownloadWikiDataAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(DownloadWikiDataAction.class);
    private static final String WIKI_URL_FORMAT =
      "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=revisions&rvprop=content&format=json";

    @SuppressWarnings("unused")
    @UseDataSet(WikipediaPipelineApp.PAGE_TITLES_DATASET)
    private KeyValueTable pages;

    @SuppressWarnings("unused")
    @UseDataSet(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET)
    private KeyValueTable wikiData;

    @Override
    public void run() {
      CloseableIterator<KeyValue<byte[], byte[]>> scanner = pages.scan(null, null);
      try {
        while (scanner.hasNext()) {
          KeyValue<byte[], byte[]> next = scanner.next();
          String rawWikiJson;
          try {
            rawWikiJson = downloadWikiData(Bytes.toString(next.getValue()));
          } catch (IOException e) {
            LOG.info("Exception while downloading wiki data {}", e.getMessage());
            continue;
          }
          wikiData.write(next.getValue(), Bytes.toBytes(rawWikiJson));
        }
      } finally {
        scanner.close();
      }
    }

    private String downloadWikiData(String page) throws IOException {
      String pageDetailsUrl = String.format(WIKI_URL_FORMAT, URLEncoder.encode(page, Charsets.UTF_8.displayName()));
      HttpRequest request = HttpRequest.get(new URL(pageDetailsUrl)).build();
      HttpResponse httpResponse = HttpRequests.execute(request);
      String responseBody = httpResponse.getResponseBodyAsString();
      if (200 != httpResponse.getResponseCode()) {
        throw new IOException(responseBody);
      }
      return responseBody;
    }
  }
}
