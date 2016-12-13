/*
 * Copyright © 2016 Cask Data, Inc.
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

package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import org.apache.twill.filesystem.Location;

import java.util.regex.Pattern;

/**
 * Post run action that deletes files in a FileSet that match a configurable regex.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(FilesetDeletePostAction.NAME)
@Description("Post run action that deletes files in a FileSet that match a configurable regex if the run succeeded.")
public class FilesetDeletePostAction extends PostAction {
  public static final String NAME = "FilesetDelete";
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String FILESET_NAME = "filesetName";
    public static final String DELETE_REGEX = "deleteRegex";
    public static final String DIRECTORY = "directory";

    @Name(FILESET_NAME)
    @Description("The fileset to delete files from.")
    private String filesetName;

    @Name(DELETE_REGEX)
    @Description("Delete files that match this regex.")
    private String deleteRegex;

    // Macro enabled properties can be set to a placeholder value ${key} when the pipeline is deployed.
    // At runtime, the value for 'key' can be given and substituted in.
    @Macro
    @Name(DIRECTORY)
    @Description("The fileset directory to delete files from.")
    private String directory;
  }

  public FilesetDeletePostAction(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Pattern.compile(config.deleteRegex);
  }

  @Override
  public void run(BatchActionContext context) throws Exception {
    if (!context.isSuccessful()) {
      return;
    }

    FileSet fileSet = context.getDataset(config.filesetName);
    Pattern pattern = Pattern.compile(config.deleteRegex);
    for (Location fileLocation : fileSet.getBaseLocation().append(config.directory).list()) {
      if (pattern.matcher(fileLocation.getName()).find()) {
        fileLocation.delete();
      }
    }
  }
}
