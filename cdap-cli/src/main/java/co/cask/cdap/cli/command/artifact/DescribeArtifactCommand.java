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

package co.cask.cdap.cli.command.artifact;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Shows information about an artifact.
 */
public class DescribeArtifactCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();
  private final ArtifactClient artifactClient;

  @Inject
  public DescribeArtifactCommand(ArtifactClient artifactClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.artifactClient = artifactClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    Id.Artifact artifactId = Id.Artifact.from(cliConfig.getCurrentNamespace(), artifactName, artifactVersion);
    String scopeStr = arguments.getOptional(ArgumentName.SCOPE.toString());

    ArtifactInfo info;
    if (scopeStr == null) {
      info = artifactClient.getArtifactInfo(artifactId);
    } else {
      ArtifactScope scope = ArtifactScope.valueOf(scopeStr.toUpperCase());
      info = artifactClient.getArtifactInfo(artifactId, scope);
    }

    Table table = Table.builder()
      .setHeader("name", "version", "scope", "app classes", "plugin classes", "properties")
      .setRows(ImmutableList.of((List<String>) ImmutableList.of(
        info.getName(),
        info.getVersion(),
        info.getScope().name(),
        GSON.toJson(info.getClasses().getApps()),
        GSON.toJson(info.getClasses().getPlugins()),
        GSON.toJson(info.getProperties())))
      )
      .build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe artifact <%s> <%s> [<%s>]",
                         ArgumentName.ARTIFACT_NAME, ArgumentName.ARTIFACT_VERSION, ArgumentName.SCOPE);
  }

  @Override
  public String getDescription() {
    return String.format("Shows information about %s. If no scope is given, the artifact is looked up first in " +
                           "SYSTEM and then in USER scope. Includes information about application and plugin " +
                           "classes contained in the artifact.",
      Fragment.of(Article.A, ElementType.ARTIFACT.getName()));
  }
}
