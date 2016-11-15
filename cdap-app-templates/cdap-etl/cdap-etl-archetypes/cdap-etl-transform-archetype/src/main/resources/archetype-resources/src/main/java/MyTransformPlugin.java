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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

/**
 * ETL Transform Plugin Shell - This provides a good starting point for building your own Transform Plugin
 * For full documentation, check out: http://docs.cask.co/cdap/current/en/hydrator-manual/developing-plugins/index.html
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("MyTransform") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is my transform")
public class MyTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {
  // If you want to log things, you will need this line
  private static final Logger LOG = LoggerFactory.getLogger(MyTransformPlugin.class);

  // Usually, you will need a private variable to store the config that was passed to your class
  private final MyConfig config;

  // This is only required for testing.
  public MyTransformPlugin(MyConfig config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published. You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
   * from being published.
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // It's usually a good idea to validate the configuration at this point. It will stop the pipeline from being
    // published if this throws an error.
    config.validate();
  }

  /**
   * This function is called when the pipeline has started. The values configured in here will be made available to the
   * transform function. Use this for initializing costly objects and opening connections that will be reused.
   * @param context Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
   * @throws Exception If there are any issues before starting the pipeline.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    // If your config options have Macros, it is usually a good idea to validate them again now that all the values
    // are substituted in.
    config.validate();
    // Initialize costly connections or large objects here which will be used in the transform.
  }

  /**
   * This is the method that is called for every record in the pipeline and allows you to make any transformations
   * you need and emit one or more records to the next stage.
   * @param input The record that is coming into the plugin
   * @param emitter An emitter allowing you to emit one or more records to the next stage
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Get all the fields from the input record
    List<Schema.Field> fields = input.getSchema().getFields();
    // Create a builder for creating the output record
    StructuredRecord.Builder builder = StructuredRecord.builder(input.getSchema());
    // Add all the values to the builder
    for (Schema.Field field : fields) {
      String name = field.getName();
      builder.set(name, input.get(name));
    }
    // If you wanted to make additional changes to the output record, this might be a good place to do it.

    // Finally, build and emit the record.
    emitter.emit(builder.build());
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }

  /**
   * Your plugin's configuration class. The fields here will correspond to the fields in the UI for configuring the
   * plugin.
   */
  public static class MyConfig extends PluginConfig {
    @Name("myOption")
    @Description("This option is required for this transform.")
    @Macro // <- Macro means that the value will be substituted at runtime by the user.
    private final String myOption;

    @Name("myOptionalOption")
    @Description("And this option is not.")
    @Macro
    @Nullable // <- This makes it optional.
    private final Integer myOptionalOption;

    public MyConfig(String myOption, Integer myOptionalOption) {
      this.myOption = myOption;
      this.myOptionalOption = myOptionalOption;
    }

    private void validate() {
      // This method should be used to validate that the configuration is valid.
      if (myOption == null || myOption.isEmpty()) {
        new IllegalArgumentException("myOption is a required field.");
      }
      // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
    }
  }
}
