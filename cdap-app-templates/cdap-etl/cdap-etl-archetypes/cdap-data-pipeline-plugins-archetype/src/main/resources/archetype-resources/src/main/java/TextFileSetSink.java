/*
 * Copyright © 2016 Cask Data, Inc.
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

package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch Sink that writes to a FileSet in text format.
 * Each record will be written as a single line, with record fields separated by a configurable separator.
 *
 * StructuredRecord is the first parameter because that is the input to the sink.
 * The second and third parameters are the key and value expected by Hadoop's {@link TextOutputFormat}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(TextFileSetSink.NAME)
@Description("Writes to a FileSet in text format.")
public class TextFileSetSink extends BatchSink<StructuredRecord, NullWritable, Text> {
  public static final String NAME = "TextFileSet";
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String FILESET_NAME = "fileSetName";
    public static final String OUTPUT_DIR = "outputDir";
    public static final String FIELD_SEPARATOR = "fieldSeparator";

    // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
    // Note:  only primitives (including boxed types) and string are the types that are supported
    @Name(FILESET_NAME)
    @Description("The name of the FileSet to write to.")
    private String fileSetName;

    // Macro enabled properties can be set to a placeholder value ${key} when the pipeline is deployed.
    // At runtime, the value for 'key' can be given and substituted in.
    @Macro
    @Name(OUTPUT_DIR)
    @Description("The FileSet directory to write to.")
    private String outputDir;

    @Nullable
    @Name(FIELD_SEPARATOR)
    @Description("The separator to use to join input record fields together. Defaults to ','.")
    private String fieldSeparator;

    // Use a no-args constructor to set field defaults.
    public Conf() {
      fileSetName = "";
      fieldSeparator = ",";
    }
  }

  // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
  public TextFileSetSink(Conf config) {
    this.config = config;
  }

  // configurePipeline is called exactly once when the pipeline is being created.
  // Any static configuration should be performed here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // create the FileSet here.
    pipelineConfigurer.createDataset(config.fileSetName,
                                     FileSet.class,
                                     FileSetProperties.builder()
                                       .setInputFormat(TextInputFormat.class)
                                       .setOutputFormat(TextOutputFormat.class)
                                       .setEnableExploreOnCreate(true)
                                       .setExploreFormat("text")
                                       .setExploreSchema("text string")
                                       .build()
    );
  }

  // prepareRun is called before every pipeline run, and is used to configure what the input should be,
  // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Map<String, String> arguments = new HashMap<>();
    FileSetArguments.setOutputPath(arguments, config.outputDir);
    context.addOutput(Output.ofDataset(config.fileSetName, arguments));
  }

  // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    // perform any actions that should happen at the end of the run.
  }

  // initialize is called by each job executor before any call to transform is made.
  // This occurs at the start of the batch job run, after the job has been successfully submitted.
  // For example, if mapreduce is the execution engine, each mapper will call initialize at the start of the program.
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // create any resources required by transform()
  }

  // destroy is called by each job executor at the end of its life.
  // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of the program.
  @Override
  public void destroy() {
    // clean up any resources created by initialize
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    StringBuilder joinedFields = new StringBuilder();
    Iterator<Schema.Field> fieldIter = input.getSchema().getFields().iterator();
    if (!fieldIter.hasNext()) {
      // shouldn't happen
      return;
    }

    Object val = input.get(fieldIter.next().getName());
    if (val != null) {
      joinedFields.append(val);
    }
    while (fieldIter.hasNext()) {
      String fieldName = fieldIter.next().getName();
      joinedFields.append(config.fieldSeparator);
      val = input.get(fieldName);
      if (val != null) {
        joinedFields.append(val);
      }
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(joinedFields.toString())));
  }

}
