/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * MapReduce job that downloads wikipedia data and stores it in a dataset.
 */
public class WikipediaDataDownloader extends AbstractMapReduce {

  public static final String NAME = co.cask.cdap.examples.wikipedia.WikipediaDataDownloader.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A MapReduce program that downloads Wikipedia data and stores it into a dataset.");
    setMapperResources(new Resources(512));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(WikipediaDataDownloaderMapper.class);
    job.setNumReduceTasks(0);
    context.addInput(Input.ofDataset(WikipediaPipelineApp.PAGE_TITLES_DATASET));
    context.addOutput(Output.ofDataset(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    WorkflowToken workflowToken = context.getWorkflowToken();
    if (workflowToken != null) {
      workflowToken.put("result", Value.of(succeeded));
    }
  }

  /**
   * Mapper that downloads Wikipedia data for each input record.
   */
  public static class WikipediaDataDownloaderMapper extends Mapper<byte [], byte [], byte [], byte []> {
    private static final Logger LOG = LoggerFactory.getLogger(WikipediaDataDownloader.class);
    private static final String WIKI_URL_FORMAT =
      "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=revisions&rvprop=content&format=json";

    @Override
    protected void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
      String rawWikiJson;
      try {
        rawWikiJson = downloadWikiData(Bytes.toString(value));
      } catch (IOException e) {
        LOG.warn("Exception while downloading wiki data {}. Skipping record.", e.getMessage());
        return;
      }

      context.write(key, Bytes.toBytes(rawWikiJson));
      context.getCounter("custom", "num.records").increment(1);
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
