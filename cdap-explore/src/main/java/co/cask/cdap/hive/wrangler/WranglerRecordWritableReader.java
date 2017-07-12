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

package co.cask.cdap.hive.wrangler;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class WranglerRecordWritableReader implements RecordReader<Void, StructuredRecordWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(WranglerRecordWritableReader.class);
  private RecordReader delegateReader;
  private Pipeline pipeline;
  private Schema outputSchema;
  private String columnName;
  private Configuration configuration;


  public <K, V> WranglerRecordWritableReader(Configuration configuration, RecordReader<K, V> delegateReader)
    throws IOException {
    this.delegateReader = delegateReader;
    this.configuration = configuration;
    try {
      initialize();
    } catch (Exception e) {
     throw new IOException(e);
    }
  }

  private void initialize() throws IOException, InterruptedException {
    Directives directives = new TextDirectives("parse-as-csv hivetext ,\n" +
                                                 "drop hivetext\n" +
                                                 "rename hivetext_1 id\n" +
                                                 "rename hivetext_2 name\n" +
                                                 "rename hivetext_3 street_address\n" +
                                                 "rename hivetext_4 city\n" +
                                                 "rename hivetext_5 state");
    // TODO Think about which context to use, also change izt based on how we restructure wrangler-core
    PipelineContext pipelineContext = new NoopPipelineContext();
    pipeline = new PipelineExecutor();
    try {
      pipeline.configure(directives, pipelineContext);
      String strSchema = "{\"type\":\"record\",\"name\":\"etlSchemaBody\"," +
        "\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\"," +
        "\"type\":[\"string\",\"null\"]},{\"name\":\"street_address\",\"type\":[\"string\",\"null\"]}," +
        "{\"name\":\"city\",\"type\":[\"string\",\"null\"]},{\"name\":\"state\",\"type\":[\"string\",\"null\"]}]}" +
        ",\"null\"]}]}";

      outputSchema = Schema.parseJson(strSchema);
      columnName = "hivetext";
    } catch (PipelineException e) {
      throw new IOException("Can not configure wrangler pipeline: ", e);
    }
  }

  @Override
  public float getProgress() throws IOException {
    // not required
    return 0;
  }

  @Override
  public boolean next(Void key, StructuredRecordWritable value) throws IOException {
    Text currentValue = new Text();
    // TODO for now we have assumed that the delegate reader will be line reader, change it to make it generic
    LOG.info("###### Before delegate reader");
    if (delegateReader.next(new LongWritable(), currentValue)) {
      try {
        Record record = new Record();
        String obj = currentValue.toString();
        LOG.info("##### Record is: {}", obj);
        record.add(columnName, obj);
        List<StructuredRecord> transformedRecords  = pipeline.execute(Arrays.asList(record), outputSchema);
        LOG.info("####### Wrangled the record");
        value.set(transformedRecords.get(0));
      } catch (PipelineException e) {
        throw new IOException("Exception while applying directives on data: ", e);
      }
      return true;
    }

    return false;
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public StructuredRecordWritable createValue() {
    return new StructuredRecordWritable();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    //no-op
  }
}
