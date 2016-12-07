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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Reader;

/**
 * Creates a stream.
 */
public class CreateStreamCommand extends AbstractAuthCommand {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private final StreamClient streamClient;

  @Inject
  public CreateStreamCommand(StreamClient streamClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String streamId = arguments.get(ArgumentName.NEW_STREAM.toString());
    StreamProperties streamProperties = null;

    if (arguments.hasArgument(ArgumentName.LOCAL_FILE_PATH.toString())) {
      File file = new File(arguments.get(ArgumentName.LOCAL_FILE_PATH.toString()));
      try (Reader reader = Files.newReader(file, Charsets.UTF_8)) {
        streamProperties = GSON.fromJson(reader, StreamProperties.class);
      } catch (FileNotFoundException e) {
        throw new IllegalArgumentException("Not a file: " + file);
      } catch (Exception e) {
        throw new IllegalArgumentException("Stream properties are malformed.", e);
      }
    }

    streamClient.create(cliConfig.getCurrentNamespace().stream(streamId), streamProperties);
    output.printf("Successfully created stream with ID '%s'\n", streamId);
  }

  @Override
  public String getPattern() {
    return String.format("create stream <%s> [<%s>]", ArgumentName.NEW_STREAM, ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getDescription() {
    return String.format("Creates %s", Fragment.of(Article.A, ElementType.STREAM.getName()));
  }
}
