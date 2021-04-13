/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.Objects;

/**
 * Connection information
 */
public class Connection {
  private final String name;
  private final String connectionType;
  private final String description;
  private final boolean preConfigured;
  private final long timestamp;
  private final ETLPlugin etlPlugin;

  public Connection(String name, String connectionType, String description, boolean preConfigured,
                    long timestamp, ETLPlugin etlPlugin) {
    this.name = name;
    this.connectionType = connectionType;
    this.description = description;
    this.preConfigured = preConfigured;
    this.timestamp = timestamp;
    this.etlPlugin = etlPlugin;
  }

  public String getName() {
    return name;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public String getDescription() {
    return description;
  }

  public boolean isPreConfigured() {
    return preConfigured;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public ETLPlugin getEtlPlugin() {
    return etlPlugin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Connection that = (Connection) o;
    return preConfigured == that.preConfigured &&
      timestamp == that.timestamp &&
      Objects.equals(name, that.name) &&
      Objects.equals(connectionType, that.connectionType) &&
      Objects.equals(description, that.description) &&
      Objects.equals(etlPlugin, that.etlPlugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, connectionType, description, preConfigured, timestamp, etlPlugin);
  }
}
