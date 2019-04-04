/*
 * Copyright © 2012-2015 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.FilePathResolver;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.common.cli.Arguments;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.lang.reflect.Type;

/**
 * Deploys an application from an existing artifact.
 */
public class UpdateAppCommand extends AbstractAuthCommand {
  private static final Type CONFIG_TYPE = new TypeToken<AppRequest<JsonObject>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final ApplicationClient applicationClient;
  private final FilePathResolver resolver;

  @Inject
  public UpdateAppCommand(ApplicationClient applicationClient, FilePathResolver resolver, CLIConfig cliConfig) {
    super(cliConfig);
    this.applicationClient = applicationClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String appName = arguments.get(ArgumentName.APP.toString());
    ApplicationId appId = cliConfig.getCurrentNamespace().app(appName);

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactScope artifactScope = ArtifactScope.valueOf(arguments.get(ArgumentName.SCOPE.toString()).toUpperCase());
    ArtifactSummary artifact = new ArtifactSummary(artifactName, artifactVersion, artifactScope);

    JsonObject config = new JsonObject();
    String configPath = arguments.getOptional(ArgumentName.APP_CONFIG_FILE.toString());
    if (configPath != null) {
      File configFile = resolver.resolvePathToFile(configPath);
      try (FileReader reader = new FileReader(configFile)) {
        AppRequest<JsonObject> appRequest = GSON.fromJson(reader, CONFIG_TYPE);
        config = appRequest.getConfig();
      }
    }

    AppRequest<JsonObject> appRequest = new AppRequest<>(artifact, config);
    applicationClient.update(appId, appRequest);
    output.println("Successfully updated application");
  }

  @Override
  public String getPattern() {
    return String.format("update app <%s> <%s> <%s> <%s> [<%s>]", ArgumentName.APP, ArgumentName.ARTIFACT_NAME,
      ArgumentName.ARTIFACT_VERSION, ArgumentName.SCOPE, ArgumentName.APP_CONFIG_FILE);
  }

  @Override
  public String getDescription() {
    return String.format("Updates %s to use another artifact version and/or configuration", Fragment.of(
      Article.A, ElementType.APP.getName()));
  }
}
