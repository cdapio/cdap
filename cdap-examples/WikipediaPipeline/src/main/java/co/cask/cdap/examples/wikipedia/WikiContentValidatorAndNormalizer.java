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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.engine.EngineException;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfig;
import org.sweble.wikitext.engine.nodes.EngProcessedPage;
import org.sweble.wikitext.engine.utils.DefaultConfigEnWp;
import org.sweble.wikitext.example.TextConverter;
import org.sweble.wikitext.parser.parser.LinkTargetException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * MapReduce program to validate and extract content from raw wikidata blobs.
 * Normalizes the data by converting the Wikitext to plain text for easier analysis in later stages.
 */
public class WikiContentValidatorAndNormalizer extends AbstractMapReduce {

  public static final String NAME = WikiContentValidatorAndNormalizer.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A MapReduce program that dumps page titles to a dataset.");
    setMapperResources(new Resources(512));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    String inputNamespace = context.getRuntimeArguments().get("input_namespace");
    String outputNamespace = context.getRuntimeArguments().get("output_namespace");
    System.out.println("ARGS input_namespace=" +  inputNamespace +  " output_namespace=" + outputNamespace);

    Job job = context.getHadoopJob();
    job.setMapperClass(FilterNormalizerMapper.class);
    job.setNumReduceTasks(0);
    if (inputNamespace != null) {
      context.addInput(Input.ofDataset(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET).fromNamespace(inputNamespace));
    } else {
      context.addInput(Input.ofDataset(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET));
    }
    if (outputNamespace != null) {
      context.addOutput(Output.ofDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET)
                          .fromNamespace(outputNamespace));
    } else {
      context.addOutput(Output.ofDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET));
    }
  }

  @Override
  public void destroy() {
    WorkflowToken workflowToken = getContext().getWorkflowToken();
    if (workflowToken != null) {
      boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
      workflowToken.put("result", Value.of(isSuccessful));
    }
  }

  /**
   * Mapper that:
   * - Filters records that are null or empty or cannot be parsed as JSON.
   * - Removes meta fields from the raw Wikipedia JSON blobs.
   * - Normalizes data formatted as Wikitext to plain text.
   */
  public static class FilterNormalizerMapper extends Mapper<byte [], byte [], byte [], byte []> {
    private static final Logger LOG = LoggerFactory.getLogger(FilterNormalizerMapper.class);
    private static final Gson GSON = new Gson();

    @Override
    protected void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
      if (key == null) {
        LOG.debug("Found null key. Skipping record.");
        return;
      }
      if (key.length == 0) {
        LOG.debug("Found empty key. Skipping record.");
        return;
      }
      if (value == null) {
        LOG.debug("Found null value. Skipping record.");
        return;
      }
      if (value.length == 0) {
        LOG.debug("Found empty value. Skipping record.");
        return;
      }
      WikiTitleAndText titleAndText;
      try {
        titleAndText = parse(value);
      } catch (JsonSyntaxException e) {
        LOG.debug("Malformed JSON found as value. Wikipedia may not have an entry for the page '{}'",
                  Bytes.toString(key));
        return;
      } catch (Exception e) {
        LOG.debug("Unable to parse the provided Wikipedia data. Skipping record.", e);
        return;
      }
      if (titleAndText == null) {
        LOG.debug("No revisions found for page in Wikipedia. Skipping record.");
        return;
      }
      String plainText;
      try {
        plainText = toPlainText(titleAndText);
      } catch (EngineException | LinkTargetException e) {
        LOG.debug("Error while parsing wikitext for '{}': '{}'. Skipping record.", Bytes.toString(key), e.getMessage());
        return;
      }
      context.write(key, Bytes.toBytes(plainText));
      context.getCounter("custom", "num.records").increment(1);
    }

    @Nullable
    private WikiTitleAndText parse(byte[] rawWikiData) {
      WikiContents json = GSON.fromJson(Bytes.toString(rawWikiData), WikiContents.class);
      Map<String, WikiContents.Query.Page> pages = json.query.pages;
      // there is only one entry in this map, with the key as the page id
      WikiContents.Query.Page page = pages.get(pages.keySet().iterator().next());
      List<WikiContents.Query.Page.Content> revisions = page.revisions;
      // we always get the latest revision
      if (revisions.isEmpty()) {
        return null;
      }
      WikiContents.Query.Page.Content content = revisions.get(revisions.size() - 1);
      return new WikiTitleAndText(page.title, content.contents);
    }

    /**
     * Converts text formatted as text/wiki into text/plain using Sweble - http://sweble.org/
     */
    private String toPlainText(WikiTitleAndText titleAndText) throws EngineException, LinkTargetException {
      // Generate a Sweble WikiConfig
      WikiConfig config = DefaultConfigEnWp.generate();
      WtEngineImpl wtEngine = new WtEngineImpl(config);
      PageTitle pageTitle = PageTitle.make(config, titleAndText.title);
      PageId pageId = new PageId(pageTitle, 0);
      // Process the text/wiki using WtEngine
      EngProcessedPage processedPage = wtEngine.postprocess(pageId, titleAndText.contents, null);
      // Use a TextConverter to convert the processed page into Text.
      // WtEngine also allows to convert to HTML, but we want to do plain text analysis.
      TextConverter plainTextConverter = new TextConverter(config, 120);
      return (String) plainTextConverter.go(processedPage.getPage());
    }

    /**
     * Class to represent response body of wikidata API at https://www.mediawiki.org/wiki/API:Query
     */
    @SuppressWarnings("unused")
    private static final class WikiContents {
      private String batchcomplete;
      private Query query;

      private static final class Query {
        private List<Normalized> normalized;

        private static final class Normalized {
          private String from;
          private String to;
        }

        private Map<String, Page> pages;

        private static final class Page {
          private long pageid;
          private long ns;
          private String title;
          private List<Content> revisions;

          private static final class Content {
            private String contentformat;
            private String contentmodel;
            @SerializedName("*")
            private String contents;
          }
        }
      }
    }

    private static final class WikiTitleAndText {
      private final String title;
      private final String contents;

      private WikiTitleAndText(String title, String contents) {
        this.title = title;
        this.contents = contents;
      }
    }
  }
}
