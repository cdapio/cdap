/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.validation;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a failure condition occurred during validation.
 */
@Beta
public class ValidationFailure {
  private static final Gson GSON = new Gson();
  private final String message;
  private final List<Cause> causes;
  private final String correctiveAction;
  private final transient String stageName;
  private final transient Map<String, Schema> inputSchemas;

  /**
   * Creates a validation failure with provided message.
   *
   * @param message validation failure message
   */
  public ValidationFailure(String message) {
    this(message, null, null, Collections.emptyMap());
  }

  /**
   * Creates a validation failure with provided message and corrective action.
   *
   * @param message validation failure message
   * @param correctiveAction corrective action
   */
  public ValidationFailure(String message, @Nullable String correctiveAction) {
    this(message, correctiveAction, null, Collections.emptyMap());
  }

  /**
   * Creates a validation failure with provided message and corrective action.
   *
   * @param message validation failure message
   * @param correctiveAction corrective action
   * @param stageName stage name
   * @param inputSchemas map of stage name to input schemas
   */
  public ValidationFailure(String message, @Nullable String correctiveAction, @Nullable String stageName,
                           Map<String, Schema> inputSchemas) {
    this.message = message;
    this.correctiveAction = correctiveAction;
    this.causes = new ArrayList<>();
    this.stageName = stageName;
    this.inputSchemas = Collections.unmodifiableMap(new HashMap<>(inputSchemas));
  }

  /**
   * Adds provided cause to this validation failure.
   *
   * @param cause cause of validation failure
   * @return validation failure with provided cause
   */
  public ValidationFailure withCause(Cause cause) {
    causes.add(cause);
    return this;
  }

  /**
   * Adds cause attributes that represents plugin not found failure cause.
   *
   * @param pluginId plugin id
   * @param pluginName plugin name
   * @param pluginType plugin type
   * @return validation failure with plugin not found cause
   */
  public ValidationFailure withPluginNotFound(String pluginId, String pluginName, String pluginType) {
    return withPluginNotFound(pluginId, pluginName, pluginType, null, null);
  }

  /**
   * Adds cause attributes that represents plugin not found failure cause.
   *
   * @param pluginId plugin id
   * @param pluginName plugin name
   * @param pluginType plugin type
   * @param requestedArtifact requested artifact
   * @param suggestedArtifact suggested artifact
   * @return validation failure with plugin not found cause
   */
  public ValidationFailure withPluginNotFound(String pluginId, String pluginName, String pluginType,
                                              @Nullable ArtifactId requestedArtifact,
                                              @Nullable ArtifactId suggestedArtifact) {
    Cause cause = new Cause().addAttribute(CauseAttributes.PLUGIN_ID, pluginId)
      .addAttribute(CauseAttributes.PLUGIN_NAME, pluginName)
      .addAttribute(CauseAttributes.PLUGIN_TYPE, pluginType);
    if (requestedArtifact != null) {
      cause.addAttribute(CauseAttributes.REQUESTED_ARTIFACT_NAME, requestedArtifact.getName());
      cause.addAttribute(CauseAttributes.REQUESTED_ARTIFACT_SCOPE, requestedArtifact.getScope().name());
      cause.addAttribute(CauseAttributes.REQUESTED_ARTIFACT_VERSION, requestedArtifact.getVersion().getVersion());
    }

    if (suggestedArtifact != null) {
      cause.addAttribute(CauseAttributes.SUGGESTED_ARTIFACT_NAME, suggestedArtifact.getName());
      cause.addAttribute(CauseAttributes.SUGGESTED_ARTIFACT_SCOPE, suggestedArtifact.getScope().name());
      cause.addAttribute(CauseAttributes.SUGGESTED_ARTIFACT_VERSION, suggestedArtifact.getVersion().getVersion());
    }
    causes.add(cause);
    return this;
  }

  /**
   * Adds cause attributes that represents invalid stage configure property failure cause.
   *
   * @param stageConfigProperty stage config property
   * @return validation failure with invalid stage config property cause
   */
  public ValidationFailure withConfigProperty(String stageConfigProperty) {
    causes.add(new Cause().addAttribute(CauseAttributes.STAGE_CONFIG, stageConfigProperty));
    return this;
  }

  /**
   * Adds cause attributes for failure cause that represents an invalid element in the list associated with given stage
   * configure property.
   *
   * @param stageConfigProperty stage config property
   * @param element element in the list associated by a given stageConfigProperty
   * @return validation failure with invalid stage config property element cause
   */
  public ValidationFailure withConfigElement(String stageConfigProperty, String element) {
    causes.add(new Cause().addAttribute(CauseAttributes.STAGE_CONFIG, stageConfigProperty)
                 .addAttribute(CauseAttributes.CONFIG_ELEMENT, element));
    return this;
  }

  /**
   * Adds cause attributes that represents invalid input schema field failure cause.
   *
   * @param fieldName name of the input schema field
   * @param inputStage stage name
   * @return validation failure with invalid input schema field cause
   */
  public ValidationFailure withInputSchemaField(String fieldName, @Nullable String inputStage) {
    Cause cause = new Cause().addAttribute(CauseAttributes.INPUT_SCHEMA_FIELD, fieldName);
    cause = inputStage == null ? cause : cause.addAttribute(CauseAttributes.INPUT_STAGE, inputStage);
    causes.add(cause);
    return this;
  }

  /**
   * Adds cause attributes that represents invalid input schema field failure cause.
   *
   * @param fieldName name of the input schema field
   * @return validation failure with invalid input schema field cause
   */
  public ValidationFailure withInputSchemaField(String fieldName) {
    if (inputSchemas.isEmpty()) {
      withInputSchemaField(fieldName, null);
    } else {
      for (String inputStage : inputSchemas.keySet()) {
        withInputSchemaField(fieldName, inputStage);
      }
    }
    return this;
  }

  /**
   * Adds cause attributes that represents invalid output schema field failure cause.
   *
   * @param fieldName name of the output schema field
   * @param outputPort stage name
   * @return validation failure with invalid output schema field cause
   */
  public ValidationFailure withOutputSchemaField(String fieldName, @Nullable String outputPort) {
    Cause cause = new Cause().addAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD, fieldName);
    cause = outputPort == null ? cause : cause.addAttribute(CauseAttributes.OUTPUT_PORT, outputPort);
    causes.add(cause);
    return this;
  }

  /**
   * Adds cause attributes that represents invalid output schema field failure cause.
   *
   * @param fieldName name of the output schema field
   * @return validation failure with invalid output schema field cause
   */
  public ValidationFailure withOutputSchemaField(String fieldName) {
    return withOutputSchemaField(fieldName, null);
  }

  /**
   * Adds cause attributes that represents a stacktrace.
   *
   * @param stacktraceElements stacktrace for the error
   * @return validation failure with stacktrace
   */
  public ValidationFailure withStacktrace(StackTraceElement[] stacktraceElements) {
    causes.add(new Cause().addAttribute(CauseAttributes.STACKTRACE, GSON.toJson(stacktraceElements)));
    return this;
  }

  /**
   * Returns failure message.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns failure message along with corrective action.
   */
  public String getFullMessage() {
    String errorMessage = message;
    if (stageName != null) {
      errorMessage = String.format("Stage '%s' encountered : %s", stageName, message);
    }
    if (correctiveAction != null) {
      return String.format("%s %s", errorMessage, correctiveAction);
    }
    return errorMessage;
  }

  /**
   * Returns corrective action for this failure.
   */
  @Nullable
  public String getCorrectiveAction() {
    return correctiveAction;
  }

  /**
   * Returns causes that caused this failure.
   */
  public List<Cause> getCauses() {
    return causes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidationFailure failure = (ValidationFailure) o;
    return message.equals(failure.message) &&
      Objects.equals(correctiveAction, failure.correctiveAction) && causes.equals(failure.causes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, correctiveAction, causes);
  }

  /**
   * Represents a cause of a failure.
   */
  @Beta
  public static class Cause {
    private final Map<String, String> attributes;

    /**
     * Creates a failure cause.
     */
    public Cause() {
      this.attributes = new LinkedHashMap<>();
    }

    /**
     * Adds an attribute to this cause.
     *
     * @param attribute cause attribute name
     * @param value cause attribute value
     * @return this cause
     */
    public Cause addAttribute(String attribute, String value) {
      attributes.put(attribute, value);
      return this;
    }

    /**
     * Returns value of the provided cause attribute.
     *
     * @param attribute attribute name
     */
    public String getAttribute(String attribute) {
      return attributes.get(attribute);
    }

    /**
     * Returns all the attributes of the cause.
     */
    public Map<String, String> getAttributes() {
      return Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Cause cause = (Cause) o;
      return attributes.equals(cause.attributes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(attributes);
    }
  }
}
