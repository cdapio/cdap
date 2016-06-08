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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * MapReduce program that dumps events from a stream to a dataset.
 */
public class StreamToDataset extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(StreamToDataset.class);

  private final String name;

  public StreamToDataset(String name) {
    this.name = name;
  }

  @Override
  public void configure() {
    setName(name);
    setDescription("A MapReduce program that dumps events from a stream to a dataset.");
    setMapperResources(new Resources(512));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Job job = context.getHadoopJob();
    job.setNumReduceTasks(0);
    WorkflowToken workflowToken = context.getWorkflowToken();
    Class<? extends Mapper> mapper = PageTitleToDatasetMapper.class;
    String inputStream = WikipediaPipelineApp.PAGE_TITLES_STREAM;
    String outputDataset = WikipediaPipelineApp.PAGE_TITLES_DATASET;
    if (workflowToken != null) {
      Value likesToDatasetResult = workflowToken.get("result", WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME);
      if (likesToDatasetResult != null && likesToDatasetResult.getAsBoolean()) {
        // The "likes" stream to the dataset has already run and has been successful in this run so far.
        // Now run raw wikipedia stream to dataset.
        mapper = RawWikiDataToDatasetMapper.class;
        inputStream = WikipediaPipelineApp.RAW_WIKIPEDIA_STREAM;
        outputDataset = WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET;
      }
    }
    LOG.info("Using '{}' as the input stream and '{}' as the output dataset.", inputStream, outputDataset);
    job.setMapperClass(mapper);
    StreamBatchReadable.useStreamInput(context, inputStream);
    context.addOutput(outputDataset);
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
   * Mapper that dumps stream events to a {@link KeyValueTable}.
   */
  public static final class PageTitleToDatasetMapper extends Mapper<LongWritable, StreamEvent, byte [], byte []> {
    private final Gson gson = new Gson();

    @Override
    protected void map(LongWritable timestamp, StreamEvent streamEvent,
                       Context context) throws IOException, InterruptedException {
      String contents = Bytes.toString(streamEvent.getBody());
      Page page = gson.fromJson(contents, Page.class);
      context.write(Bytes.toBytes(page.getId()), Bytes.toBytes(page.getName()));
      // Increment the same counter from all map-reduce programs so we can use them for verification via
      // Workflow Token in tests as well as Condition Node Predicates where applicable.
      context.getCounter("custom", "num.records").increment(1);
    }

    @VisibleForTesting
    static class Page {
      private final String name;
      private final String id;
      @SuppressWarnings("unused")
      @SerializedName("created_time")
      private final String createdTime;

      Page(String name, String id, String createdTime) {
        this.name = name;
        this.id = id;
        this.createdTime = createdTime;
      }

      public String getName() {
        return name;
      }

      public String getId() {
        return id;
      }
    }
  }

  /**
   * Mapper that dumps raw Wikipedia data from a stream to a {@link KeyValueTable}.
   */
  public static final class RawWikiDataToDatasetMapper extends Mapper<LongWritable, StreamEvent, byte [], byte []> {
    @Override
    protected void map(LongWritable key, StreamEvent streamEvent,
                       Context context) throws IOException, InterruptedException {
      String contents = Bytes.toString(streamEvent.getBody());
      context.write(Bytes.toBytes(UUID.randomUUID()), Bytes.toBytes(contents));
      context.getCounter("custom", "num.records").increment(1);
    }
  }
}
