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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.common.RecordWritableConverter;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;

/**
 * A {@link BatchSource} that writes data to Elasticsearch.
 * <p>
 * This {@link ElasticsearchSource} reads from an Elasticsearch index and type and converts the MapWritable
 * into a {@link StructuredRecord} and emits the StructuredRecord.
 * </p>
 * An exception will be thrown if the type of any of the fields do not match the type specified by the user.
 */
@Plugin(type = "batchsource")
@Name("Elasticsearch")
@Description("CDAP Elasticsearch Batch Source pulls documents from Elasticsearch " +
  "according to the query specified by the user and converts each document to a structured record " +
  "with the fields and schema specified by the user. " +
  "The Elasticsearch server should be running prior to creating the adapter.")
public class ElasticsearchSource extends BatchSource<Text, MapWritable, StructuredRecord> {
  private static final String INDEX_DESCRIPTION = "The name of the index to query.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data is stored.";
  private static final String QUERY_DESCRIPTION = "The query to use to import data from the specified index. " +
    "See Elasticsearch for query examples.";
  private static final String HOST_DESCRIPTION = "The hostname and port for the Elasticsearch instance; " +
    "for example, localhost:9200.";
  private static final String SCHEMA_DESCRIPTION = "The schema or mapping of the data in Elasticsearch.";

  private final ESConfig config;
  private Schema schema;

  public ElasticsearchSource(ESConfig config) {
    this.config = config;
  }

  private String getResource() {
    return String.format("%s/%s", config.index, config.type);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    schema = parseSchema();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setSpeculativeExecution(false);
    Configuration conf = job.getConfiguration();
    conf.set("es.nodes", config.hostname);
    conf.set("es.resource", getResource());
    conf.set("es.query", config.query);
    job.setInputFormatClass(EsInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
  }

  @Override
  public void transform(KeyValue<Text, MapWritable> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(RecordWritableConverter.convertToRecord(input.getValue(), schema));
  }

  private Schema parseSchema() {
    try {
      return Strings.isNullOrEmpty(config.schema) ? null : Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  /**
   * Config class for Batch {@link ElasticsearchSource}.
   */
  public static class ESConfig extends PluginConfig {
    @Name(Properties.Elasticsearch.HOST)
    @Description(HOST_DESCRIPTION)
    private String hostname;

    @Name(Properties.Elasticsearch.INDEX_NAME)
    @Description(INDEX_DESCRIPTION)
    private String index;

    @Name(Properties.Elasticsearch.TYPE_NAME)
    @Description(TYPE_DESCRIPTION)
    private String type;

    @Name(Properties.Elasticsearch.QUERY)
    @Description(QUERY_DESCRIPTION)
    private String query;

    @Name(Properties.Elasticsearch.SCHEMA)
    @Description(SCHEMA_DESCRIPTION)
    private String schema;

    public ESConfig(String hostname, String index, String type, String query, String schema) {
      this.hostname = hostname;
      this.index = index;
      this.type = type;
      this.schema = schema;
      this.query = query;
    }
  }
}
