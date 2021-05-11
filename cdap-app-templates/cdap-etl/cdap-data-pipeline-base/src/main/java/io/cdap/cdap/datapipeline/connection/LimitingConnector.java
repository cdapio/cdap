/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.connection;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.batch.preview.LimitingInputFormat;
import io.cdap.cdap.etl.batch.preview.LimitingInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Limiting connector to read from a batch connector
 */
public class LimitingConnector implements DirectConnector {
  private final BatchConnector batchConnector;

  public LimitingConnector(BatchConnector batchConnector) {
    this.batchConnector = batchConnector;
  }

  @Override
  public List<StructuredRecord> sample(SampleRequest request) throws IOException {
    InputFormatProvider inputFormatProvider = batchConnector.getInputFormatProvider(request);

    // use limiting format to read from the input format
    Map<String, String> configs =
      LimitingInputFormatProvider.getConfiguration(inputFormatProvider, request.getLimit());
    Configuration hConf = new Configuration();
    configs.forEach(hConf::set);

    Job job = Job.getInstance(hConf);
    job.setJobID(new JobID("sample", 0));
    LimitingInputFormat<?, ?> inputFormat = new LimitingInputFormat<>();

    List<InputSplit> splits;
    try {
      splits = inputFormat.getSplits(job);
    } catch (InterruptedException e) {
      throw new IOException(String.format("Unable to get the splits from the input format %s",
                                          inputFormatProvider.getInputFormatClassName()));
    }

    List<StructuredRecord> sample = new ArrayList<>();
    // limiting format only has 1 split
    InputSplit split = splits.get(0);
    TaskID taskId = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(hConf, new TaskAttemptID(taskId, 0));

    // create record reader to read the results
    try (RecordReader<?, ?> reader = inputFormat.createRecordReader(split, taskContext)) {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        sample.add(batchConnector.transform(reader.getCurrentKey(), reader.getCurrentValue()));
      }
    } catch (InterruptedException e) {
      throw new IOException(String.format("Unable to read the values from the input format %s",
                                          inputFormatProvider.getInputFormatClassName()));
    }
    return sample;
  }

  @Override
  public void test(FailureCollector collector) throws ValidationException {
    batchConnector.test(collector);
  }

  @Override
  public BrowseDetail browse(BrowseRequest request) throws IOException {
    return batchConnector.browse(request);
  }

  @Override
  public ConnectorSpec generateSpec(String path) {
    return batchConnector.generateSpec(path);
  }

  @Override
  public void configure(PluginConfigurer configurer) {
    batchConnector.configure(configurer);
  }

  @Override
  public void close() throws IOException {
    batchConnector.close();
  }
}
