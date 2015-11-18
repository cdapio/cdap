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
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Loads an artifact into CDAP.
 */
public class SetArtifactPropertiesCommand extends AbstractAuthCommand {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final ArtifactClient artifactClient;
  private final FilePathResolver resolver;

  @Inject
  public SetArtifactPropertiesCommand(ArtifactClient artifactClient, CLIConfig cliConfig, FilePathResolver resolver) {
    super(cliConfig);
    this.artifactClient = artifactClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    String scopeStr = arguments.get(ArgumentName.SCOPE.toString());
    ArtifactScope scope = ArtifactScope.valueOf(scopeStr.toUpperCase());

    Id.Namespace namespace = scope == ArtifactScope.SYSTEM ? Id.Namespace.SYSTEM : cliConfig.getCurrentNamespace();
    Id.Artifact artifactId = Id.Artifact.from(namespace, artifactName, artifactVersion);

    String propertiesFilePath = arguments.get(ArgumentName.LOCAL_FILE_PATH.toString());
    File propertiesFile = resolver.resolvePathToFile(propertiesFilePath);
    try (Reader reader = new FileReader(propertiesFile)) {
      Map<String, String> properties;
      try {
        properties = GSON.fromJson(reader, MAP_TYPE);
      } catch (Exception e) {
        throw new RuntimeException("Error parsing file contents as a JSON Object. " +
                                     "Please check that it is a valid JSON Object.", e);
      }
      artifactClient.writeProperties(artifactId, properties);
    }
  }

  @Override
  public String getPattern() {
    return String.format("set artifact properties <%s> <%s> <%s> <%s>",
                         ArgumentName.ARTIFACT_NAME, ArgumentName.ARTIFACT_VERSION,
                         ArgumentName.SCOPE, ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getDescription() {
    return String.format(
      "Sets properties of %s. " +
        "If a file is specified, the file must contain a JSON Object of the properties for the artifact. " +
        "If a directory is specified, every file in the directory is added as a property, with the filename " +
        "as the property key and the file contents as the property value.",
      Fragment.of(Article.A, ElementType.ARTIFACT.getName()));
  }
}
