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

import io.cdap.cdap.api.annotation.Beta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents an error condition occurred during validation.
 */
@Beta
public class ValidationFailure {
  // types of the failures
  private static final String STAGE_ERROR = "StageError";
  private static final String INVALID_PROPERTY = "InvalidProperty";
  private static final String PLUGIN_NOT_FOUND = "PluginNotFound";

  // represents stage name in the failure. It is a generic property used in all the failures for a given stage
  private static final String STAGE = "stage";
  // represents configuration property in InvalidProperty failure
  private static final String CONFIG_PROPERTY = "configProperty";
  // represents plugin id in PluginNotFound failure
  private static final String PLUGIN_ID = "pluginId";
  // represents plugin type in PluginNotFound failure
  private static final String PLUGIN_TYPE = "pluginType";
  // represents plugin name in PluginNotFound failure
  private static final String PLUGIN_NAME = "pluginName";

  private final String message;
  private final String type;
  private final String correctiveAction;
  private final Map<String, Object> properties;

  private ValidationFailure(String message, String type, @Nullable String correctiveAction,
                            Map<String, Object> properties) {
    this.message = message;
    this.type = type;
    this.correctiveAction = correctiveAction;
    this.properties = properties;
  }

  /**
   * Creates a stage validation failure. This failure should be added to the stage configurer when there is a generic
   * failure related to a stage such as not able to connect to underlying sink/source.
   *
   * @param message validation failure message
   * @param stage stage name
   * @param correctiveAction corrective action
   * @return a stage validation failure
   */
  public static ValidationFailure createStageFailure(String message, String stage, @Nullable String correctiveAction) {
    Builder builder = builder(message, STAGE_ERROR);
    builder.setCorrectiveAction(correctiveAction).addProperty(STAGE, stage);
    return builder.build();
  }

  /**
   * Creates a config property validation failure. This failure should be added to the stage configurer when the
   * provided stage configuration property is invalid.
   *
   * @param message validation failure message
   * @param stage stage name
   * @param correctiveAction corrective action
   * @return a config property validation failure
   */
  public static ValidationFailure createConfigPropertyFailure(String message, String stage,
                                                              String property, @Nullable String correctiveAction) {
    Builder builder = builder(message, INVALID_PROPERTY);
    builder.setCorrectiveAction(correctiveAction)
      .addProperty(STAGE, stage).addProperty(CONFIG_PROPERTY, property);
    return builder.build();
  }

  /**
   * Creates a plugin not found validation failure. This failure should be added to the stage configurer when the
   * plugin is not found while validating the stage.
   *
   * @param message validation failure message
   * @param stage stage name
   * @param correctiveAction corrective action
   * @return a plugin not found failure
   */
  public static ValidationFailure createPluginNotFoundFailure(String message, String stage,
                                                              String pluginId, String pluginName, String pluginType,
                                                              @Nullable String correctiveAction) {
    Builder builder = builder(message, PLUGIN_NOT_FOUND);
    builder.setCorrectiveAction(correctiveAction)
      .addProperty(STAGE, stage).addProperty(PLUGIN_ID, pluginId)
      .addProperty(PLUGIN_TYPE, pluginType)
      .addProperty(PLUGIN_NAME, pluginName);
    return builder.build();
  }

  /**
   * Returns a builder for creating a {@link ValidationFailure}.
   */
  public static Builder builder(String message, String type) {
    return new Builder(message, type);
  }

  /**
   * A builder to create {@link ValidationFailure} instance.
   */
  public static class Builder {
    private final String message;
    private final String type;
    private String correctiveAction;
    private final Map<String, Object> properties;

    private Builder(String message, String type) {
      this.message = message;
      this.type = type;
      this.properties = new HashMap<>();
    }

    /**
     * Sets corrective action to rectify the failure.
     *
     * @param correctiveAction corrective action
     * @return this builder
     */
    public Builder setCorrectiveAction(String correctiveAction) {
      this.correctiveAction = correctiveAction;
      return this;
    }

    /**
     * Adds a property to the failure.
     *
     * @param property the name of the property
     * @param value the value of the property
     * @return this builder
     */
    public Builder addProperty(String property, String value) {
      this.properties.put(property, value);
      return this;
    }

    /**
     * Creates a new instance of {@link ValidationFailure}.
     *
     * @return instance of {@link ValidationFailure}
     */
    public ValidationFailure build() {
      return new ValidationFailure(message, type, correctiveAction,
                                   Collections.unmodifiableMap(new HashMap<>(properties)));
    }
  }

  /**
   * Returns message of this failure.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns type of this failure.
   */
  public String getType() {
    return type;
  }

  /**
   * Returns corrective action for this failure.
   */
  @Nullable
  public String getCorrectiveAction() {
    return correctiveAction;
  }

  /**
   * Returns properties of this failure.
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidationFailure that = (ValidationFailure) o;
    return message.equals(that.message) && type.equals(that.type) &&
      Objects.equals(correctiveAction, that.correctiveAction) &&
      properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, type, correctiveAction, properties);
  }

  @Override
  public String toString() {
    return "ValidationFailure{" +
      "message='" + message + '\'' +
      ", type='" + type + '\'' +
      ", correctiveAction='" + correctiveAction + '\'' +
      ", properties=" + properties +
      '}';
  }
}
