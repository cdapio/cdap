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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordStringConversion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

/**
 * A {@link BatchSink} that writes data to a Elasticsearch.
 * <p/>
 * This {@link ElasticsearchSink} takes a {@link StructuredRecord} in and converts it to a json per {@link StructuredRecordStringConversion},
 * and writes it to the Elasticserach server.
 * <p/>
 * If the Elasticserach index does not exist, it will be created using the default properties
 * specified by Elasticsearch. See more information at
 * {@link https://www.elastic.co/guide/en/elasticsearch/guide/current/_index_settings.html}.
 * <p/>
 */
@Plugin(type = "sink")
@Name("Elasticsearch")
@Description("CDAP Elasticsearch Batch Sink")
public class ElasticsearchSink extends BatchSink<StructuredRecord, Writable, Writable> {
  private static final String INDEX_DESC = "The name of the index where the data will be stored. " +
    "The index should already exist and be configured.";

  private static final String TYPE_DESC = "The name of the type where the data will be stored";

  private static final String ID_DESC = "The id field that will determine the id for the document. " +
    "It should match a fieldname in the structured record of the input";

  private static final String HOST_DESC = "The hostname and port for an elasticsearch node. e.g. localhost:9300";

  /**
   * Config class for RealtimeTableSink
   */
  public static class ESConfig extends PluginConfig {
    @Name(Properties.Elasticsearch.HOST)
    @Description(HOST_DESC)
    private String hostname;

    @Name(Properties.Elasticsearch.INDEX_NAME)
    @Description(INDEX_DESC)
    private String index;

    @Name(Properties.Elasticsearch.TYPE_NAME)
    @Description(TYPE_DESC)
    private String type;

    @Name(Properties.Elasticsearch.ID_FIELD)
    @Description(ID_DESC)
    private String idField;

    public ESConfig(String hostname, String index, String type, String idField) {
      this.hostname = hostname;
      this.index = index;
      this.type = type;
      this.idField = idField;
    }
  }

  private final ESConfig config;

  public ElasticsearchSink(ESConfig config) {
    this.config = config;
  }

  private String getResource() {
    return String.format("%s/%s", config.index, config.type);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    conf.set("es.nodes", config.hostname);
    conf.set("es.resource", getResource());
    conf.set("es.input.json", "yes");
    conf.set("es.mapping.id", config.idField);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(EsOutputFormat.class);
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<Writable, Writable>> emitter) throws Exception {
    emitter.emit(new KeyValue<Writable, Writable>(new Text(StructuredRecordStringConversion.toJsonString(record)),
                                                  new Text(StructuredRecordStringConversion.toJsonString(record))));
  }
}
