/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Gets properties for an artifact.
 */
public class GetArtifactPropertiesCommand extends AbstractAuthCommand {
  private final ArtifactClient artifactClient;

  @Inject
  public GetArtifactPropertiesCommand(ArtifactClient artifactClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.artifactClient = artifactClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactId artifactId = cliConfig.getCurrentNamespace().artifact(artifactName, artifactVersion);
    String scopeStr = arguments.getOptional(ArgumentName.SCOPE.toString());

    ArtifactInfo info;
    if (scopeStr == null) {
      info = artifactClient.getArtifactInfo(artifactId);
    } else {
      ArtifactScope scope = ArtifactScope.valueOf(scopeStr.toUpperCase());
      info = artifactClient.getArtifactInfo(artifactId, scope);
    }

    List<Map.Entry<String, String>> rows = new ArrayList<>(info.getProperties().size());
    rows.addAll(info.getProperties().entrySet());
    Table table = Table.builder()
      .setHeader("key", "value")
      .setRows(rows, new RowMaker<Map.Entry<String, String>>() {
        @Override
        public List<String> makeRow(Map.Entry<String, String> entry) {
          List<String> columns = new ArrayList<>(2);
          columns.add(entry.getKey());
          columns.add(entry.getValue());
          return columns;
        }
      })
      .build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get artifact properties <%s> <%s> [<%s>]",
                         ArgumentName.ARTIFACT_NAME, ArgumentName.ARTIFACT_VERSION, ArgumentName.SCOPE);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the properties of %s. If no scope is provided, properties are looked for first in " +
                         "the 'SYSTEM' and then in the 'USER' scope.",
                         Fragment.of(Article.A, ElementType.ARTIFACT.getName()));
  }
}
