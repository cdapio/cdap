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
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Specification that is used to configure an adapter.
 */
public final class  AdapterSpecification {

  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final Set<Source> sources;
  private final Set<Sink> sinks;

  /**
   * Construct Adapter specification with the given parameters.
   *
   * @param name  Name of the adapter.
   * @param type  Adapter type.
   * @param properties Properties for configuring the adapter.
   * @param sources {@link List} of {@Source}s used by the adapter.
   * @param sinks {@link List} of {Sink}s used by the adapter.
   */
  public AdapterSpecification(String name, String type, Map<String, String> properties, Set<Source> sources,
                              Set<Sink> sinks) {
    this.name = name;
    this.type = type;
    this.properties = ImmutableMap.copyOf(properties);
    this.sources = ImmutableSet.copyOf(sources);
    this.sinks = ImmutableSet.copyOf(sinks);
  }

  /**
   * @return {@link Set} of {@link Source}s configured for an Adapter.
   */
  public Set<Source> getSources() {
    return sources;
  }

  /**
   * @return {@link Set} of {@link Sink}s configured for an Adapter.
   */
  public Set<Sink> getSinks() {
    return sinks;
  }

  /**
   * @return type of Adapter.
   */
   public String getType() {
    return type;
   }

  /**
   * @return name of the Adapter.
   */
  public String getName() {
    return name;
  }

  /**
   * @return An immutable {@link Map} of properties.
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @return name of the schedule for this Adapter.
   */
  public String getScheduleName() {
    // For now, simply schedule the adapter's program with the name of the adapter.
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterSpecification that = (AdapterSpecification) o;

    if (name.equals(that.name) && properties.equals(that.properties) && sinks.equals(that.sinks) &&
        sources.equals(that.sources) && type.equals(that.type)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.type, this.properties, this.sources, this.sinks);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("type", type)
      .add("properties", properties)
      .add("sources", sources)
      .add("sinks", sinks)
      .toString();
  }
}
