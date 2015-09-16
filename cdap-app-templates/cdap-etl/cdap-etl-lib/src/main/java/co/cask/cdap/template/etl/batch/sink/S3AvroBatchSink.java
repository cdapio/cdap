package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 * {@link S3AvroBatchSink} that stores data in avro format to S3.
 */
@Plugin(type = "batchsink")
@Name("S3AvroSink")
@Description("Sink for a S3 that writes data in Avro format.")
public class S3AvroBatchSink extends S3BatchSink<AvroKey<GenericRecord>, NullWritable> {

  private StructuredToAvroTransformer recordTransformer;
  private final S3AvroSinkConfig config;

  private static final String SCHEMA_DESC = "The Avro schema of the record being written to the Sink as a JSON " +
    "Object.";

  public S3AvroBatchSink(S3AvroSinkConfig config) {
    super(config);
    this.config = config;
  }


  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    super.prepareRun(context);
    Schema avroSchema = new Schema.Parser().parse(config.schema);
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
    context.addOutput(config.name, new S3AvroOutputFormatProvider(config));
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Configuration for the S3AvroSink.
   */
  public static class S3AvroSinkConfig extends S3BatchSinkConfig {

    @Name(Properties.S3BatchSink.SCHEMA)
    @Description(SCHEMA_DESC)
    private String schema;

    public S3AvroSinkConfig(String name, String basePath, String schema,
                            String accessID, String accessKey) {
      super(name, basePath, accessID, accessKey);
      this.schema = schema;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class S3AvroOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public S3AvroOutputFormatProvider(S3AvroSinkConfig config) {
      conf = Maps.newHashMap();
      conf.put("input.format", AvroKeyInputFormat.class.getName());
      conf.put("output.format", AvroKeyOutputFormat.class.getName());
    }

    @Override
    public String getOutputFormatClassName() {
      return AvroKeyOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
