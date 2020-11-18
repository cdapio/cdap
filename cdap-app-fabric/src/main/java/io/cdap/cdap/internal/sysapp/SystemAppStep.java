/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.sysapp;

import com.google.gson.JsonObject;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Definition for a single step in a system app config. Defines what operation to perform for a
 * system app and any arguments required to perform the operation.
 */
public class SystemAppStep {

  private final String label;
  private final Type type;
  private final Arguments arguments;

  public SystemAppStep(String label, Type type, Arguments arguments) {
    this.label = label;
    this.type = type;
    this.arguments = arguments;
  }

  public String getLabel() {
    return label;
  }

  public Type getType() {
    return type;
  }

  public Arguments getArguments() {
    return arguments;
  }

  /**
   * Validate that all required fields exist.
   *
   * @throws IllegalArgumentException if the step is invalid
   */
  public void validate() {
    if (label == null || label.isEmpty()) {
      throw new IllegalArgumentException("A capability step must contain a label.");
    }

    if (type == null) {
      throw new IllegalArgumentException("A capability step must contain a type.");
    }

    arguments.validate();
  }

  /**
   * System app step type
   */
  public enum Type {
    ENABLE_SYSTEM_APP
  }

  /**
   * Arguments required to create an application
   */
  static class Arguments extends AppRequest<JsonObject> {

    private String namespace;
    private String name;
    private boolean overwrite;

    Arguments(AppRequest<JsonObject> appRequest, String namespace, String name, boolean overwrite) {
      super(appRequest.getArtifact(), appRequest.getConfig(), appRequest.getPreview(),
            appRequest.getOwnerPrincipal(),
            appRequest.canUpdateSchedules());
      this.namespace = namespace;
      this.name = name;
      this.overwrite = overwrite;
    }

    public ApplicationId getId() {
      return new NamespaceId(namespace).app(name);
    }

    public void validate() {
      super.validate();
      if (namespace == null || namespace.isEmpty()) {
        throw new IllegalArgumentException("Namespace must be specified");
      }
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Application name must be specified");
      }
      getId();
    }

    public boolean isOverwrite() {
      return overwrite;
    }
  }
}
