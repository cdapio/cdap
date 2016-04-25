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
package org.example.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Batch Source that reads from a FileSet that has its data formatted as text.
 *
 * LongWritable is the first parameter because that is the key used by Hadoop's {@link TextInputFormat}.
 * Similarly, Text is the second parameter because that is the value used by Hadoop's {@link TextInputFormat}.
 * {@link StructuredRecord} is the third parameter because that is what the source will output.
 * All the plugins included with Hydrator operate on StructuredRecord.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(TextFileSetSource.NAME)
@Description("Reads from a FileSet that has its data formatted as text.")
public class TextFileSetSource extends BatchSource<LongWritable, Text, StructuredRecord> {
  public static final String NAME = "TextFileSet";
  public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "textRecord",
    Schema.Field.of("position", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("text", Schema.of(Schema.Type.STRING))
  );
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String FILESET_NAME = "fileSetName";
    public static final String CREATE_IF_NOT_EXISTS = "createIfNotExists";
    public static final String DELETE_INPUT_ON_SUCCESS = "deleteInputOnSuccess";

    // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
    // Note:  only primitives (including boxed types) and string are the types that are supported
    @Name(FILESET_NAME)
    @Description("The name of the FileSet to read from.")
    private String fileSetName;

    // A nullable fields tells CDAP that this is an optional field.
    @Nullable
    @Name(CREATE_IF_NOT_EXISTS)
    @Description("Whether to create the FileSet if it doesn't already exist. Defaults to false.")
    private Boolean createIfNotExists;

    @Nullable
    @Name(DELETE_INPUT_ON_SUCCESS)
    @Description("Whether to delete the data read by the source after the run succeeds. Defaults to false.")
    private Boolean deleteInputOnSuccess;

    // Use a no-args constructor to set field defaults.
    public Conf() {
      fileSetName = "";
      createIfNotExists = false;
      deleteInputOnSuccess = false;
    }
  }

  // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
  public TextFileSetSource(Conf config) {
    this.config = config;
  }

  // configurePipeline is called exactly once when the pipeline is being created.
  // Any static configuration should be performed here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // if the user has set createIfNotExists to true, create the FileSet here.
    if (config.createIfNotExists) {
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
    // set the output schema of this stage so that stages further down the pipeline will know their input schema.
    pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
  }

  // prepareRun is called before every pipeline run, and is used to configure what the input should be,
  // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    context.setInput(Input.ofDataset(config.fileSetName));
  }

  // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    // perform any actions that should happen at the end of the run.
    // in our case, we want to delete the data read during this run if the run succeeded.
    if (succeeded && config.deleteInputOnSuccess) {
      FileSet fileSet = context.getDataset(config.fileSetName);
      for (Location inputLocation : fileSet.getInputLocations()) {
        try {
          inputLocation.delete(true);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
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

  // transform is used to transform the key-value pair output by the input into objects output by this source.
  // The output should be a StructuredRecord if you want the source to be compatible with the plugins included
  // with Hydrator.
  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(OUTPUT_SCHEMA)
                   .set("position", input.getKey().get())
                   .set("text", input.getValue().toString())
                   .build()
    );
  }
}


