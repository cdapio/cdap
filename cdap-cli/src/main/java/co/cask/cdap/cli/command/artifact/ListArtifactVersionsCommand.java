/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all versions of a specific artifact.
 */
public class ListArtifactVersionsCommand extends AbstractAuthCommand {

  private final ArtifactClient artifactClient;

  @Inject
  public ListArtifactVersionsCommand(ArtifactClient artifactClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.artifactClient = artifactClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String scopeStr = arguments.getOptional(ArgumentName.SCOPE.toString());
    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    List<ArtifactSummary> artifactSummaries;
    if (scopeStr == null) {
      artifactSummaries = artifactClient.listVersions(cliConfig.getCurrentNamespace(), artifactName);
    } else {
      ArtifactScope scope = ArtifactScope.valueOf(scopeStr.toUpperCase());
      artifactSummaries = artifactClient.listVersions(cliConfig.getCurrentNamespace(), artifactName, scope);
    }

    Table table = Table.builder()
      .setHeader("name", "version", "scope")
      .setRows(artifactSummaries, new RowMaker<ArtifactSummary>() {
        @Override
        public List<?> makeRow(ArtifactSummary object) {
          return Lists.newArrayList(object.getName(), object.getVersion(), object.getScope().name());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list artifact versions <%s> [<%s>]", ArgumentName.ARTIFACT_NAME, ArgumentName.SCOPE);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all versions of a specific %s. If no scope is provided, %s are looked " +
                           "for first in the SYSTEM and then in the USER scope.",
                         ElementType.ARTIFACT.getName(), ElementType.ARTIFACT.getNamePlural());
  }
}
