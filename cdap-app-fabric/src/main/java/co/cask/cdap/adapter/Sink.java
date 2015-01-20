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

package co.cask.cdap.adapter;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Defines an Adapter Sink.
 */
public final class Sink {

  /**
   * Defines the Sink Type.
   */
  public enum Type {
    /**
     * Defines the sink type to be Dataset
     */
    DATASET
  }

  private final String name;
  private final Type type;
  private final Map<String, String> properties;

  /**
   * Construct a Sink with the given parameters.
   *
   * @param name  Name of the Sink.
   * @param type  Name of the Sink.
   * @param properties {@Map} of properties associated with the Sink.
   */
  public Sink(String name, Type type, Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.properties = ImmutableMap.copyOf(properties);
  }

  /**
   * @return name of the Sink.
   */
  public String getName() {
    return name;
  }

  /**
   * @return type of the Sink.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return {@link Map} of properties.
   */
  public Map<String, String> getProperties() {
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

    Sink sink = (Sink) o;

    return (name.equals(sink.name) && properties.equals(sink.properties) &&  type == sink.type);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.type, this.properties);
  }
}
