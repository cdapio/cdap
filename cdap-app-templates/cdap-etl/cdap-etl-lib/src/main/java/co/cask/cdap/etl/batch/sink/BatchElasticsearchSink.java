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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BatchSink} that writes data to an Elasticsearch server.
 * <p/>
 * This {@link BatchElasticsearchSink} takes a {@link StructuredRecord} in,
 * converts it to a JSON string per {@link StructuredRecordStringConverter},
 * and writes it to the Elasticsearch server.
 * <p/>
 * If the Elasticsearch index does not exist, it will be created using the default properties
 * specified by Elasticsearch. See more information at
 * https://www.elastic.co/guide/en/elasticsearch/guide/current/_index_settings.html.
 * <p/>
 */
@Plugin(type = "batchsink")
@Name("Elasticsearch")
@Description("CDAP Elasticsearch Batch Sink takes the structured record from the input source and converts it " +
  "to a JSON string, then indexes it in Elasticsearch using the index, type, and id specified by the user. " +
  "The Elasticsearch server should be running prior to creating the adapter.")
public class BatchElasticsearchSink extends BatchSink<StructuredRecord, Writable, Writable> {
  private static final String INDEX_DESCRIPTION = "The name of the index where the data will be stored. " +
    "If the index does not already exist, it will be created using Elasticsearch's default properties.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data will be stored. " +
    "If it does not already exist, it will be created.";
  private static final String ID_DESCRIPTION = "The field that will determine the id for the document. " +
    "It should match a fieldname in the structured record of the input.";
  private static final String HOST_DESCRIPTION = "The hostname and port for the Elasticsearch server; " +
    "such as localhost:9200.";

  private final ESConfig config;

  public BatchElasticsearchSink(ESConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    job.setSpeculativeExecution(false);
    context.addOutput(config.index, new ElasticSearchOutputFormatProvider(config));
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<Writable, Writable>> emitter) throws Exception {
    emitter.emit(new KeyValue<Writable, Writable>(new Text(StructuredRecordStringConverter.toJsonString(record)),
                                                  new Text(StructuredRecordStringConverter.toJsonString(record))));
  }

  /**
   * Config class for Batch BatchElasticsearchSink
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

    @Name(Properties.Elasticsearch.ID_FIELD)
    @Description(ID_DESCRIPTION)
    private String idField;

    public ESConfig(String hostname, String index, String type, String idField) {
      this.hostname = hostname;
      this.index = index;
      this.type = type;
      this.idField = idField;
    }
  }

  private static class ElasticSearchOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    public ElasticSearchOutputFormatProvider(ESConfig config) {
      this.conf = new HashMap<>();

      conf.put("es.nodes", config.hostname);
      conf.put("es.resource", String.format("%s/%s", config.index, config.type));
      conf.put("es.input.json", "yes");
      conf.put("es.mapping.id", config.idField);
    }

    @Override
    public String getOutputFormatClassName() {
      return EsOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
