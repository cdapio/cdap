/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import java.util.Objects;

/**
 * Base class for identifiers of properties from the triggering pipeline.
 */
public abstract class TriggeringPipelinePropertyId {

  /**
   * The type of the triggering pipeline property
   */
  public enum Type {
    RUNTIME_ARG,
    PLUGIN_PROPERTY,
    TOKEN
  }

  private final Type type;
  private final String namespace;
  private final String pipelineName;

  public TriggeringPipelinePropertyId(Type type, String namespace, String pipelineName) {
    this.type = type;
    this.namespace = namespace;
    this.pipelineName = pipelineName;
  }

  /**
   * @return Type of the triggering pipeline property.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return Namespace of the triggering pipeline.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return Names of the triggering pipeline.
   */
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TriggeringPipelinePropertyId that = (TriggeringPipelinePropertyId) o;

    return Objects.equals(getType(), that.getType()) &&
      Objects.equals(getNamespace(), that.getNamespace()) &&
      Objects.equals(getPipelineName(), that.getPipelineName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getNamespace(), getPipelineName());
  }
}
