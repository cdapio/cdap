/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.TextDirectives;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class WranglerRecordWritableReader extends RecordReader<Void, StructuredRecordWritable> {
  private RecordReader delegateReader;
  private Void key;
  private StructuredRecordWritable value;
  private Directives directives;
  private Pipeline pipeline;
  private PipelineContext pipelineContext;
  private Schema outputSchema;
  private String columnName;

  public WranglerRecordWritableReader(RecordReader delegateReader) {
    this.delegateReader = delegateReader;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegateReader.initialize(split, context);
    Configuration jobConf = context.getConfiguration();
    directives = new TextDirectives(jobConf.get("wrangler.directives"));
    // TODO Think about which context to use, also change it based on how we restructure wrangler-core
    pipelineContext = new NoopPipelineContext();
    pipeline = new PipelineExecutor();
    try {
      pipeline.configure(directives, pipelineContext);
      outputSchema = Schema.parseJson(jobConf.get("wrangler.output.schema"));
      columnName = jobConf.get("wrangler.output.schema");
    } catch (PipelineException e) {
      throw new IOException("Can not configure wrangler pipeline: ", e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (delegateReader.nextKeyValue()) {
      // TODO for now we have assumed that the delegate reader will be line reader, change it to make it generic
      Text currentValue = (Text) delegateReader.getCurrentValue();
      try {
        Record record = new Record();
        String obj = currentValue.toString();
        record.add(columnName, obj);
        List<StructuredRecord> transformedRecords  = pipeline.execute(Arrays.asList(record), outputSchema);

        value = new StructuredRecordWritable(transformedRecords.get(0));
      } catch (PipelineException e) {
        throw new IOException("Exception while applying directives on data: ", e);
      }
      return true;
    }
    value = null;
    return false;
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StructuredRecordWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    //no-op
  }
}
