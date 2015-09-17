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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.common.Properties;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for {@link FileBatchSink}.
 */
public abstract class FileSetSinkConfig extends PluginConfig {
  @Name(Properties.SnapshotFileSet.NAME)
  @Description(FileBatchSink.NAME_DESC)
  protected String name;

  @Name(Properties.SnapshotFileSet.BASE_PATH)
  @Description(FileBatchSink.BASE_PATH_DESC)
  @Nullable
  protected String basePath;

  public FileSetSinkConfig(String name, @Nullable String basePath) {
    this.name = name;
    this.basePath = basePath;
  }
}
