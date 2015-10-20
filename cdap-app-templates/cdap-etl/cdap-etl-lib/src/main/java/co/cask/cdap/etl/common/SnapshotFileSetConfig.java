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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for snapshot fileset sources and sinks.
 */
public abstract class SnapshotFileSetConfig extends PluginConfig {
  @Name(Properties.SnapshotFileSetSink.NAME)
  @Description("Name of the PartitionedFileset Dataset to which the records are written to. " +
    "If it doesn't exist, it will be created.")
  protected String name;

  @Name(Properties.SnapshotFileSetSink.BASE_PATH)
  @Description("The path where the data will be recorded. " +
    "Defaults to the name of the dataset.")
  @Nullable
  protected String basePath;

  @Name(Properties.SnapshotFileSetSink.FILE_PROPERTIES)
  @Nullable
  @Description("Advanced feature to specify any additional properties that should be used with the sink, " +
    "specified as a JSON object of string to string. These properties are set on the dataset if one is created. " +
    "The properties are also passed to the dataset at runtime as arguments.")
  protected String fileProperties;

  public SnapshotFileSetConfig(String name, @Nullable String basePath, @Nullable String fileProperties) {
    this.name = name;
    this.basePath = basePath;
    this.fileProperties = fileProperties;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getBasePath() {
    return basePath;
  }

  @Nullable
  public String getFileProperties() {
    return fileProperties;
  }
}
