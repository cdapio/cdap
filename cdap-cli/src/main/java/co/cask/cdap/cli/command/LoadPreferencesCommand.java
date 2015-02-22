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
import co.cask.cdap.client.PreferencesClient;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.ConversionException;

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
    printStream.printf(SUCCESS + "\n", type.getPrettyName());
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

    if (contentType.isEmpty() || !(contentType.equals("json") || contentType.equals("xml"))) {
      throw new IllegalArgumentException("Unsupported file format. Only json and xml formats are supported");
    }

    FileReader reader = new FileReader(file);
    Map<String, String> args = Maps.newHashMap();
    try {
      if (contentType.equals("json")) {
        args = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
      } else {
        XStream xStream = new XStream();
        xStream.alias("map", Map.class);
        args = (Map<String, String>) xStream.fromXML(reader);
      }
    } catch (JsonSyntaxException e) {
      throw new BadRequestException(
        String.format("Json Syntax in File is invalid. Support only for string-to-string map. %s", e.getMessage()));
    } catch (ConversionException e) {
      throw new BadRequestException(
        String.format("XML Syntax in File is invalid. Support only for string-to-string map. %s", e.getMessage()));
    } finally {
      reader.close();
    }

    if (arguments.hasArgument(type.getArgumentName().toString())) {
      programIdParts = arguments.get(type.getArgumentName().toString()).split("\\.");
    }
    setPreferences(programIdParts, printStream, args);
  }

  @Override
  public String getPattern() {
    return String.format("load %s preferences <%s> <%s> [<%s>]", type.getName(), ArgumentName.LOCAL_FILE_PATH,
                         ArgumentName.CONTENT_TYPE, type.getArgumentName());
  }

  @Override
  public String getDescription() {
    return String.format("Set Preferences of a %s from a local Config File (supported formats = JSON).", 
                         type.getPluralPrettyName());
  }
}
