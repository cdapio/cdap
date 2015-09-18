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

package co.cask.cdap.cli.command.app;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.lang.reflect.Type;

/**
 * Deploys an application from an existing artifact.
 */
public class CreateAppCommand extends AbstractAuthCommand {
  private static final Type configType = new TypeToken<AppRequest<JsonObject>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final ApplicationClient applicationClient;
  private final FilePathResolver resolver;

  @Inject
  public CreateAppCommand(ApplicationClient applicationClient, FilePathResolver resolver, CLIConfig cliConfig) {
    super(cliConfig);
    this.applicationClient = applicationClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String appName = arguments.get(ArgumentName.APP.toString());
    Id.Application appId = Id.Application.from(cliConfig.getCurrentNamespace(), appName);

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactScope artifactScope = ArtifactScope.valueOf(arguments.get(ArgumentName.SCOPE.toString()).toUpperCase());
    ArtifactSummary artifact = new ArtifactSummary(artifactName, artifactVersion, artifactScope);

    JsonObject config = new JsonObject();
    String configPath = arguments.getOptional(ArgumentName.APP_CONFIG_FILE.toString());
    if (configPath != null) {
      File configFile = resolver.resolvePathToFile(configPath);
      try (FileReader reader = new FileReader(configFile)) {
        AppRequest<JsonObject> appRequest = GSON.fromJson(reader, configType);
        config = appRequest.getConfig();
      }
    }

    AppRequest<JsonObject> appRequest = new AppRequest<>(artifact, config);
    applicationClient.deploy(appId, appRequest);
    output.println("Successfully created application");
  }

  @Override
  public String getPattern() {
    return String.format("create app <%s> <%s> <%s> <%s> [<%s>]", ArgumentName.APP, ArgumentName.ARTIFACT_NAME,
                         ArgumentName.ARTIFACT_VERSION, ArgumentName.SCOPE, ArgumentName.APP_CONFIG_FILE);
  }

  @Override
  public String getDescription() {
    return String.format("Creates %s from an artifact with optional configuration. If configuration is needed, it " +
      "must be given as a file whose contents are a JSON Object containing the application config. " +
      "For example, the file contents could contain: '{ \"config\": { \"stream\": \"purchases\" } }'. In this case, " +
      "the application would recieve '{ \"stream\": \"purchases\" }' as its config object.",
      Fragment.of(Article.A, ElementType.APP.getName()));
  }
}
