/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.cli.command.artifact;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.List;

/**
 * Lists all plugin types available to an artifact.
 */
public class ListArtifactPluginTypesCommand extends AbstractAuthCommand {

  private final ArtifactClient artifactClient;

  @Inject
  public ListArtifactPluginTypesCommand(ArtifactClient artifactClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.artifactClient = artifactClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactId artifactId = cliConfig.getCurrentNamespace().artifact(artifactName, artifactVersion);

    List<String> types;
    String scopeStr = arguments.getOptional(ArgumentName.SCOPE.toString());
    if (scopeStr == null) {
      types = artifactClient.getPluginTypes(artifactId);
    } else {
      types = artifactClient.getPluginTypes(artifactId,
          ArtifactScope.valueOf(scopeStr.toUpperCase()));
    }

    Table table = Table.builder()
        .setHeader("plugin type")
        .setRows(types, new RowMaker<String>() {
          @Override
          public List<?> makeRow(String object) {
            return Lists.newArrayList(object);
          }
        }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list artifact plugin-types <%s> <%s> [<%s>]",
        ArgumentName.ARTIFACT_NAME, ArgumentName.ARTIFACT_VERSION, ArgumentName.SCOPE);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all plugin types usable by the specified %s. "
            + "If no scope is provided, %s are looked for first in the 'SYSTEM' and then in the 'USER' scope.",
        ElementType.ARTIFACT.getName(), ElementType.ARTIFACT.getNamePlural());
  }
}
