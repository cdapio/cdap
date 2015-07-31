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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordStringConverter;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link BatchSource} that writes data to a Elasticsearch.
 * <p/>
 * This {@link ElasticsearchSource} reads from an Elasticsearch index and type and converts the MapWritable
 * into an {@link StructuredRecord} and emits the StructuredRecord.
 * <p/>
 * An exception will be thrown if the type of any of the fields does not match the type stated by the user.
 */
@Plugin(type = "source")
@Name("Elasticsearch")
@Description("CDAP Elasticsearch Batch Sink takes the structured record from the input source" +
  " and converts it to a json, then indexes it in elasticsearch using the index, type, and id specified by the user." +
  "The elasticsearch server should be running prior to creating the adapter.")
public class ElasticsearchSource extends BatchSource<Text, MapWritable, StructuredRecord> {
  private static final String INDEX_DESC = "The name of the index to query";
  private static final String TYPE_DESC = "The name of the type where the data is stored.";
  private static final String QUERY_DESC = "The query to use to import data from the specified index. " +
    "See elasticsearch for query examples";
  private static final String HOST_DESC = "The hostname and port for the elasticsearch instance, e.g. localhost:9200";
  private static final String SCHEMA_DESC = "The schema or mapping of the data in elasticsearch";

  private final ESConfig config;
  private Schema schema;

  public ElasticsearchSource(ESConfig config) {
    this.config = config;
  }

  private String getResource() {
    return String.format("%s/%s", config.index, config.type);
  }

  @Override
  public void initialize(BatchSourceContext context) {
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
    emitter.emit(readRecord(input.getValue(), schema == null ? parseSchema() : schema));
  }

  private Schema parseSchema() {
    try {
      return Strings.isNullOrEmpty(config.schema) ? null : Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  private static StructuredRecord readRecord(MapWritable input, Schema schema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      try {
        builder.set(field.getName(), readWritables(input.get(new Text(field.getName())), field.getSchema()));
      } catch (Exception e) {
        throw(new IOException(String.format("Type exception for field %s: %s",
                                                   field.getName(), e.getMessage())));
      }
    }
    return builder.build();
  }

  private static Object readWritables(Writable writable, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        if (writable.getClass() == NullWritable.class) {
          return null;
        }
        throw new ClassCastException("This field is not null:" + writable.toString());
      case BOOLEAN:
        return ((BooleanWritable) writable).get();
      case INT:
        //Downcasting is necessary because Elasticsearch defaults to storing all ints as longs
        return (int) (writable.getClass() == IntWritable.class ? ((IntWritable) writable).get() :
          ((LongWritable) writable).get());
      case LONG:
        return ((LongWritable) writable).get();
      case FLOAT:
        // Downcasting is necessary because Elasticsearch defaults to storing all floats as doubles
        return (float) (writable.getClass() == FloatWritable.class ? ((FloatWritable) writable).get() :
          ((DoubleWritable) writable).get());
      case DOUBLE:
        return ((DoubleWritable) writable).get();
      case BYTES:
        return ((ByteWritable) writable).get();
      case STRING:
        return writable.toString();
      case ENUM:
        // Currently there is no standard container to represent enum type
        return writable.toString();
      case ARRAY:
        return readArray((ArrayWritable) writable, schema.getComponentSchema());
      case MAP:
        return readMap((MapWritable) writable, schema.getMapSchema());
      case RECORD:
        return readRecord((MapWritable) writable, schema);
      case UNION:
        return readUnion(writable, schema);
    }
    throw new IOException("Unsupported schema: " + schema);
  }

  private static List<Object> readArray(ArrayWritable input, Schema elementSchema) throws IOException {
    List<Object> result = new ArrayList<>();
    for (Writable writable : input.get()) {
      result.add(readWritables(writable, elementSchema));
    }
    return result;
  }

  private static Map<Object, Object> readMap(MapWritable input,
                                             Map.Entry<Schema, Schema> mapSchema) throws IOException {
    Schema keySchema = mapSchema.getKey();
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }

    Schema valueSchema = mapSchema.getValue();
    Map<Object, Object> result = new HashMap<>();

    for (Writable key : input.keySet()) {
      result.put(readWritables(key, keySchema), readWritables(input.get(key), valueSchema));
    }
    return result;
  }

  private static Object readUnion(Writable input, Schema unionSchema) throws IOException {
      for (Schema schema : unionSchema.getUnionSchemas()) {
        try {
          return readWritables(input, schema);
        } catch (ClassCastException e) {
          //no-op; keep iterating until the appropriate class is found
        }
      }
      throw new IOException("No matching schema found for union type: " + unionSchema);
  }

  /**
   * Config class for Batch ElasticsearchSink
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

    @Name(Properties.Elasticsearch.QUERY)
    @Description(QUERY_DESC)
    private String query;

    @Name(Properties.Elasticsearch.SCHEMA)
    @Description(SCHEMA_DESC)
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
