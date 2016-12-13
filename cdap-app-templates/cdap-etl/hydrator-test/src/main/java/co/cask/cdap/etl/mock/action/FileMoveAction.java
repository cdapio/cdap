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

package co.cask.cdap.etl.mock.action;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import org.apache.twill.filesystem.Location;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Action that moves files that match some regex.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(FileMoveAction.NAME)
@Description("Action that moves files from one fileset into another, optionally filtering files that match a regex.")
public class FileMoveAction extends Action {
  public static final String NAME = "FileMove";
  private final Conf conf;

  public FileMoveAction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(conf.destinationFileset, FileSet.class);
    Pattern.compile(conf.filterRegex);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        FileSet sourceFileSet = context.getDataset(conf.sourceFileset);
        FileSet destinationFileSet = context.getDataset(conf.destinationFileset);

        Pattern pattern = Pattern.compile(conf.filterRegex);

        for (Location sourceFile : sourceFileSet.getBaseLocation().list()) {
          if (pattern.matcher(sourceFile.getName()).matches()) {
            Location destFile = destinationFileSet.getBaseLocation().append(sourceFile.getName());
            sourceFile.renameTo(destFile);
          }
        }
      }
    });
  }

  /**
   * Conf for the token writer.
   */
  public static class Conf extends PluginConfig {
    @Description("The fileset to delete files from.")
    private String sourceFileset;

    @Description("The path to the files to delete")
    private String destinationFileset;

    @Nullable
    @Description("Filter any files whose name matches this regex. Defaults to '^\\.', which will filter any files " +
      "that begin with a period.")
    private String filterRegex;

    // set defaults for properties in a no-argument constructor.
    public Conf() {
      filterRegex = "^\\.";
    }
  }
}
