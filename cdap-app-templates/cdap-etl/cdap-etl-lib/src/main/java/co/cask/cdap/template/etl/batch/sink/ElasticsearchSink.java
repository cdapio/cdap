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
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.RecordUtils;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An ElasticsearchSink
 */
@Plugin(type = "sink")
@Name("Elasticsearch")
@Description("CDAP Elasticsearch Batch Sink")
public class ElasticsearchSink extends BatchSink<StructuredRecord, Writable, Writable> {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

  private static final String CLUSTER_DESC = "The name of the elasticsearch cluster. " +
    "Defaults to the elasticsearch default: \"elasticsearch\"";

  private static final String INDEX_DESC = "The name of the index where the data will be stored. " +
    "The index should already exist and be configured.";

  private static final String PROPERTY_SCHEMA_DESC = "The mapping that will be applied to the data";

  private static final String TYPE_DESC = "The name of the type where the data will be stored";

  private static final String ID_DESC = "The id field that will determine the id for the document. " +
    "Defaults to a unique ID created by elasticsearch";

  private static final String HOST_DESC = "The hostname and port for an elasticsearch node. e.g. localhost:9300";

  private static final String SERVER_DESC = "The elasticsearch server to connect to";

  private static final String PORT_DESC = "The port to connect to, e.g. 9200";

  /**
   * Config class for RealtimeTableSink
   */
  public static class ESConfig extends PluginConfig {
    @Name(Properties.Elasticsearch.INDEX_NAME)
    @Description(INDEX_DESC)
    private String index;

    @Name(Properties.Elasticsearch.TYPE_NAME)
    @Description(TYPE_DESC)
    private String type;

    @Name(Properties.Elasticsearch.SCHEMA)
    @Description(PROPERTY_SCHEMA_DESC)
    @Nullable
    private String schemaStr;

    @Name(Properties.Elasticsearch.ID_FIELD)
    @Description(ID_DESC)
    @Nullable
    private String idField;

    @Name(Properties.Elasticsearch.HOST)
    @Description(HOST_DESC)
    @Nullable
    private String hostname;

    @Name(Properties.Elasticsearch.SERVER)
    @Description(SERVER_DESC)
    private String server;

    @Name(Properties.Elasticsearch.PORT)
    @Description(PORT_DESC)
    private String port;

    public ESConfig(String server, String port, String index, String type, String schemaStr,
                    String idField, String hostname) {
      this.index = index;
      this.type = type;
      this.schemaStr = schemaStr;
      this.idField = idField;
      this.hostname = hostname;
      this.server = server;
      this.port = port;
    }
  }

  private final ESConfig config;

  public ElasticsearchSink(ESConfig config) {
    this.config = config;
  }

  private String getNodes() {
    return String.format("%s:%s", config.server, config.port);
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
    conf.set("es.nodes", getNodes());
    conf.set("es.resource", getResource());
    conf.set("es.input.json", "yes");
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(EsOutputFormat.class);
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<Writable, Writable>> emitter) throws Exception {
    emitter.emit(new KeyValue<Writable, Writable>(new Text(RecordUtils.toJsonString(record)),
                                                  new Text(RecordUtils.toJsonString(record))));
  }
}
