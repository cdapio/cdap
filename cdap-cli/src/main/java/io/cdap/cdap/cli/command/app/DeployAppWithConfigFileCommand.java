/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.FilePathResolver;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.common.cli.Arguments;

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * Deploys an application given a config file.
 */
public class DeployAppWithConfigFileCommand extends AbstractAuthCommand {
  private final ApplicationClient applicationClient;
  private final FilePathResolver resolver;

  @Inject
  public DeployAppWithConfigFileCommand(ApplicationClient applicationClient, FilePathResolver resolver,
                                        CLIConfig cliConfig) {
    super(cliConfig);
    this.applicationClient = applicationClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    File file = resolver.resolvePathToFile(arguments.get(ArgumentName.APP_JAR_FILE.toString()));
    Preconditions.checkArgument(file.exists(), "File " + file.getAbsolutePath() + " does not exist");
    Preconditions.checkArgument(file.canRead(), "File " + file.getAbsolutePath() + " is not readable");

    String configPath = arguments.getOptional(ArgumentName.APP_CONFIG_FILE.toString());
    StringBuilder configString = new StringBuilder();
    File configFile = resolver.resolvePathToFile(configPath);
    List<String> lines = Files.readAllLines(configFile.getAbsoluteFile().toPath(), StandardCharsets.UTF_8);
    for (String line: lines) {
      configString.append(line);
    }
    applicationClient.deploy(cliConfig.getCurrentNamespace(), file, configString.toString());

    output.println("Successfully deployed application");
  }

  @Override
  public String getPattern() {
    return String.format("deploy app <%s> with config <%s>", ArgumentName.APP_JAR_FILE, ArgumentName.APP_CONFIG_FILE);
  }

  @Override
  public String getDescription() {
    return String.format("Deploys %s, optionally with a configuration file", Fragment.of(Article.A,
                                                                                         ElementType.APP.getName()));
  }
}
