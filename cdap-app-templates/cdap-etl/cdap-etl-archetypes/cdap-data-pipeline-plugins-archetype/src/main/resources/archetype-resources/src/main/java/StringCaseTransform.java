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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(StringCaseTransform.NAME)
@Description("Transforms configured fields to lowercase or uppercase.")
public class StringCaseTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "StringCase";
  private final Conf config;
  private Set<String> upperFields;
  private Set<String> lowerFields;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String UPPER_FIELDS = "upperFields";
    public static final String LOWER_FIELDS = "lowerFields";
    private static final Pattern SPLIT_ON = Pattern.compile("\\s*,\\s*");

    // nullable means this property is optional
    @Nullable
    @Name(UPPER_FIELDS)
    @Description("A comma separated list of fields to uppercase. Each field must be of type String.")
    private String upperFields;

    @Nullable
    @Name(LOWER_FIELDS)
    @Description("A comma separated list of fields to lowercase. Each field must be of type String.")
    private String lowerFields;

    private Set<String> getUpperFields() {
      return parseToSet(upperFields);
    }

    private Set<String> getLowerFields() {
      return parseToSet(lowerFields);
    }

    private Set<String> parseToSet(String str) {
      Set<String> set = new HashSet<>();
      if (str == null || str.isEmpty()) {
        return set;
      }
      for (String element : SPLIT_ON.split(str)) {
        set.add(element);
      }
      return set;
    }
  }

  public StringCaseTransform(Conf config) {
    this.config = config;
  }

  // configurePipeline is called only once, when the pipeline is deployed. Static validation should be done here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    // Failure collector is api used to collect all the validation errors
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    // the output schema is always the same as the input schema
    Schema inputSchema = stageConfigurer.getInputSchema();

    // if schema is null, that means it is either not known until runtime, or it is variable
    if (inputSchema != null) {
      // if the input schema is constant and known at configure time, check that all configured fields are strings
      for (String fieldName : config.getUpperFields()) {
        validateFieldIsString(inputSchema, failureCollector, fieldName);
      }
      for (String fieldName : config.getLowerFields()) {
        validateFieldIsString(inputSchema, failureCollector, fieldName);
      }
    }
    // Throw an exception before setting output schema
    failureCollector.getOrThrowException();

    stageConfigurer.setOutputSchema(inputSchema);
  }

  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    upperFields = config.getUpperFields();
    lowerFields = config.getLowerFields();
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(record.getSchema());
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      if (upperFields.contains(fieldName)) {
        builder.set(fieldName, record.get(fieldName).toString().toUpperCase());
      } else if (lowerFields.contains(fieldName)) {
        builder.set(fieldName, record.get(fieldName).toString().toLowerCase());
      } else {
        builder.set(fieldName, record.get(fieldName));
      }
    }
    emitter.emit(builder.build());
  }

  private void validateFieldIsString(Schema schema, FailureCollector failureCollector, String fieldName) {
    Schema.Field inputField = schema.getField(fieldName);
    if (inputField == null) {
      failureCollector.addFailure(String.format("Field '%s' must be present in input schema.", fieldName),
                                  null);
      return;
    }
    Schema fieldSchema = inputField.getSchema();
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    if (fieldType != Schema.Type.STRING) {
      failureCollector.addFailure(String.format("Field '%s' is of invalid type %s.", fieldName, fieldType),
                                  "It must be of type string.");
    }
  }
}
