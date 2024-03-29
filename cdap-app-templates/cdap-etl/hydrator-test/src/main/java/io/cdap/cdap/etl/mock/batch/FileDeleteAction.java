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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import java.util.regex.Pattern;
import org.apache.twill.filesystem.Location;

/**
 * Post run action that deletes files in a FileSet that match a configurable regex.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(FileDeleteAction.NAME)
@Description("Post run action that deletes files in a FileSet that match a configurable regex.")
public class FileDeleteAction extends PostAction {

  public static final String NAME = "FileDelete";
  private final Conf conf;

  public FileDeleteAction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(conf.filesetName, FileSet.class);
    Pattern.compile(conf.deleteRegex);
  }

  @Override
  public void run(BatchActionContext context) throws Exception {
    if (!context.isSuccessful()) {
      return;
    }

    FileSet fileSet = context.getDataset(conf.filesetName);
    Pattern pattern = Pattern.compile(conf.deleteRegex);
    for (Location fileLocation : fileSet.getBaseLocation().list()) {
      if (pattern.matcher(fileLocation.getName()).matches()) {
        fileLocation.delete();
      }
    }
  }

  /**
   * Conf for the token writer.
   */
  public static class Conf extends PluginConfig {

    @Description("The fileset to delete files from.")
    private String filesetName;

    @Description("Delete files that match this regex.")
    private String deleteRegex;
  }
}
