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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.client.PreferencesClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Command to load a config property file.
 */
public class LoadPreferencesCommand extends AbstractSetPreferencesCommand {
  private static final String SUCCESS = "Load Preferences File was successful for the '%s'";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final ElementType type;

  protected LoadPreferencesCommand(ElementType type, PreferencesClient client, CLIConfig cliConfig) {
    super(type, client, cliConfig);
    this.type = type;
  }

  @Override
  public void printSuccessMessage(PrintStream printStream, ElementType type) {
    printStream.printf(SUCCESS + "\n", type.getTitleName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = new String[0];
    String contentType = arguments.get(ArgumentName.CONTENT_TYPE.toString(), "");
    File file = new File(arguments.get(ArgumentName.LOCAL_FILE_PATH.toString()));

    if (!file.isFile()) {
      throw new IllegalArgumentException("Not a file: " + file);
    }

    Map<String, String> args = Maps.newHashMap();
    try (FileReader reader = new FileReader(file)) {
      if (contentType.equals("json")) {
        args = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
      } else {
        throw new IllegalArgumentException("Unsupported file format. Only JSON format is supported");
      }
    } catch (JsonSyntaxException e) {
      throw new BadRequestException(
        String.format("JSON syntax in file is invalid. Support only for string-to-string map. %s", e.getMessage()));
    }

    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }
    setPreferences(programIdParts, printStream, args);
  }

  @Override
  public String getPattern() {
    return determinePattern("load");
  }

  @Override
  public String getDescription() {
    return String.format("Sets preferences of %s from a local config file (supported formats = JSON).",
                         Fragment.of(Article.A, type.getTitleName()));
  }
}
