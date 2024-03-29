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

package io.cdap.cdap.cli.command.app;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lists all applications.
 */
public class ListAppsCommand extends AbstractAuthCommand {

  private final ApplicationClient appClient;

  @Inject
  public ListAppsCommand(ApplicationClient appClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.appClient = appClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String artifactNamesStr = arguments.getOptional(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.getOptional(ArgumentName.ARTIFACT_VERSION.toString());
    Set<String> artifactNames = new HashSet<>();
    if (artifactNamesStr != null) {
      for (String name : Splitter.on(',').trimResults().split(artifactNamesStr)) {
        artifactNames.add(name);
      }
    }
    Table table = Table.builder()
      .setHeader("id", "appVersion", "description", "artifactName", "artifactVersion", "artifactScope", "principal")
      .setRows(appClient.list(cliConfig.getCurrentNamespace(), artifactNames, artifactVersion, null),
        new RowMaker<ApplicationRecord>() {
          @Override
          public List<?> makeRow(ApplicationRecord object) {
            return Lists.newArrayList(object.getName(), object.getAppVersion(), object.getDescription(),
              object.getArtifact().getName(), object.getArtifact().getVersion(), object.getArtifact().getScope(),
              object.getOwnerPrincipal());
          }
        }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list apps [<%s>] [<%s>]", ArgumentName.ARTIFACT_NAME,
        ArgumentName.ARTIFACT_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s, optionally filtered by artifact name and version",
        ElementType.APP.getNamePlural());
  }
}
