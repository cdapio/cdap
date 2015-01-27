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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;

/**
 * Sets the properties of a stream from a local file.
 */
public class SetStreamPropertiesCommand extends AbstractAuthCommand {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private final StreamClient streamClient;

  @Inject
  public SetStreamPropertiesCommand(StreamClient streamClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.STREAM.toString());
    File file = new File(arguments.get(ArgumentName.LOCAL_FILE_PATH.toString()));

    if (!file.isFile()) {
      throw new IllegalArgumentException("Not a file: " + file);
    }

    Reader reader = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8);
    StreamProperties streamProperties;
    try {
      streamProperties = GSON.fromJson(new FileReader(file), StreamProperties.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Stream properties are malformed.", e);
    } finally {
      reader.close();
    }

    streamClient.setStreamProperties(streamId, streamProperties);
    output.printf("Successfully set properties of stream '%s'\n", streamId);
  }

  @Override
  public String getPattern() {
    return String.format("set stream properties <%s> <%s>", ArgumentName.STREAM, ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getDescription() {
    return "Sets the properties of a stream, such as TTL, format, and notification threshold.";
  }
}
