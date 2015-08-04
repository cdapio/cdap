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

package co.cask.cdap.cli.command.adapter;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;

/**
 * Creates an adapter.
 */
public class CreateAdapterCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();

  private final AdapterClient adapterClient;
  private final FilePathResolver filePathResolver;

  @Inject
  public CreateAdapterCommand(AdapterClient adapterClient, CLIConfig cliConfig,
                              FilePathResolver filePathResolver) {
    super(cliConfig);
    this.adapterClient = adapterClient;
    this.filePathResolver = filePathResolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String adapterName = arguments.get(ArgumentName.ADAPTER.toString());
    File adapterConfigFile = filePathResolver.resolvePathToFile(
      arguments.get(ArgumentName.ADAPTER_SPEC.toString()));

    try (FileReader fileReader = new FileReader(adapterConfigFile)) {
      adapterClient.create(Id.Adapter.from(cliConfig.getCurrentNamespace(), adapterName),
                           GSON.fromJson(fileReader, AdapterConfig.class));
      output.printf("Successfully created adapter '%s'\n", adapterName);
    }
  }

  @Override
  public String getPattern() {
    return String.format("create adapter <%s> <%s>", ArgumentName.ADAPTER, ArgumentName.ADAPTER_SPEC);
  }

  @Override
  public String getDescription() {
    return String.format("Creates %s.", Fragment.of(Article.A, ElementType.ADAPTER.getName()));
  }
}
