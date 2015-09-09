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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.common.Properties;

/**
 * {@link PluginConfig} for {@link FileSetSink}.
 */
public abstract class FileSetSinkConfig extends PluginConfig {
  @Name(Properties.SnapshotFileSet.NAME)
  @Description(FileSetSink.NAME_DESC)
  protected String name;

  @Name(Properties.SnapshotFileSet.PATH)
  @Description(FileSetSink.PATH_DESC)
  protected String path;

  public FileSetSinkConfig(String name, String path) {
    this.name = name;
    this.path = path;
  }
}
