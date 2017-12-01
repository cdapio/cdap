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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.common.conf.ArtifactConfig;
import co.cask.cdap.common.conf.ArtifactConfigReader;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.cli.Arguments;
import com.google.common.io.Files;
import com.google.inject.Inject;

import java.io.File;
import java.io.PrintStream;
import java.util.Map;

/**
 * Loads an artifact into CDAP.
 */
public class LoadArtifactCommand extends AbstractAuthCommand {
  private final ArtifactClient artifactClient;
  private final FilePathResolver resolver;
  private final ArtifactConfigReader configReader;

  @Inject
  public LoadArtifactCommand(ArtifactClient artifactClient, CLIConfig cliConfig, FilePathResolver resolver) {
    super(cliConfig);
    this.artifactClient = artifactClient;
    this.resolver = resolver;
    this.configReader = new ArtifactConfigReader();
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    File artifactFile = resolver.resolvePathToFile(arguments.get(ArgumentName.LOCAL_FILE_PATH.toString()));

    String name = arguments.getOptional(ArgumentName.ARTIFACT_NAME.toString());
    String version = arguments.getOptional(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactId artifactId;
    if (name == null && version != null) {
      throw new IllegalArgumentException("If a version is specified, name must also be specified.");
    } else if (name != null && version == null) {
      throw new IllegalArgumentException("If a name is specified, a version must also be specified.");
    } else if (name == null) {
      artifactId = new ArtifactId(cliConfig.getCurrentNamespace().getNamespace(), artifactFile.getName());
    } else {
      artifactId = cliConfig.getCurrentNamespace().artifact(name, version);
    }

    String configPath = arguments.getOptional(ArgumentName.ARTIFACT_CONFIG_FILE.toString());
    NamespaceId namespace = artifactId.getParent();
    if (configPath == null) {
      artifactClient.add(namespace, artifactId.getEntityName(),
                         Files.newInputStreamSupplier(artifactFile), artifactId.getVersion());
    } else {
      File configFile = resolver.resolvePathToFile(configPath);
      ArtifactConfig artifactConfig = configReader.read(Id.Namespace.fromEntityId(namespace), configFile);
      artifactClient.add(namespace, artifactId.getEntityName(),
                         Files.newInputStreamSupplier(artifactFile), artifactId.getVersion(),
                         artifactConfig.getParents(), artifactConfig.getPlugins());

      Map<String, String> properties = artifactConfig.getProperties();
      if (properties != null && !properties.isEmpty()) {
        artifactClient.writeProperties(artifactId, properties);
      }
    }

    output.printf("Successfully added artifact with name '%s'\n", artifactId.getEntityName());
  }

  @Override
  public String getPattern() {
    return String.format("load artifact <%s> [config-file <%s>] [name <%s>] [version <%s>]",
                         ArgumentName.LOCAL_FILE_PATH, ArgumentName.ARTIFACT_CONFIG_FILE,
                         ArgumentName.ARTIFACT_NAME, ArgumentName.ARTIFACT_VERSION);
  }

  @Override
  public String getDescription() {
    return "Loads an artifact into CDAP. If the artifact name and version are not both given, " +
      "they will be derived from the filename of the artifact. " +
      "File names are expected to be of the form '<name>-<version>.jar'. " +
      "If the artifact contains plugins that extend another artifact, or if it contains " +
      "third-party plugins, a config file must be provided. " +
      "The config file must contain a JSON object that specifies the parent artifacts " +
      "and any third-party plugins in the JAR.\n" +
      "\n" +
      "For example, if there is a config file with these contents:\n" +
      "\n" +
      "    {\n" +
      "      \"parents\":[ \"app1[1.0.0,2.0.0)\", \"app2[1.2.0,1.3.0] ],\n" +
      "      \"plugins\":[\n" +
      "        { \"type\": \"jdbc\",\n" +
      "          \"name\": \"mysql\",\n" +
      "          \"className\": \"com.mysql.jdbc.Driver\"\n" +
      "        }\n" +
      "      ],\n" +
      "      \"properties\":{\n" +
      "        \"prop1\": \"val1\"\n" +
      "      }\n" +
      "    }\n" +
      "\n" +
      "This config specifies that the artifact contains one JDBC third-party plugin that should be " +
      "available to the 'app1' artifact (versions 1.0.0 inclusive to 2.0.0 exclusive) and 'app2' artifact " +
      "(versions 1.2.0 inclusive to 1.3.0 inclusive). The config may also include a 'properties' field specifying " +
      "properties for the artifact.";
  }
}
