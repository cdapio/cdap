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

package co.cask.cdap.cli.command.stream.view;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates or updates a stream view.
 */
public class CreateOrUpdateStreamViewCommand extends AbstractAuthCommand {

  private final StreamViewClient client;

  @Inject
  public CreateOrUpdateStreamViewCommand(StreamViewClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    StreamViewId viewId = streamId.view(arguments.get(ArgumentName.VIEW.toString()));
    String formatName = arguments.get(ArgumentName.FORMAT.toString());
    Schema schema = getSchema(arguments);

    Map<String, String> settings = Collections.emptyMap();
    if (arguments.hasArgument(ArgumentName.SETTINGS.toString())) {
      settings = ArgumentParser.parseMap(arguments.get(ArgumentName.SETTINGS.toString()));
    }
    FormatSpecification formatSpecification = new FormatSpecification(formatName, schema, settings);
    ViewSpecification viewSpecification = new ViewSpecification(formatSpecification);

    boolean created = client.createOrUpdate(viewId.toId(), viewSpecification);
    if (created) {
      output.printf("Successfully created stream-view '%s'\n", viewId.getEntityName());
    } else {
      output.printf("Successfully updated stream-view '%s'\n", viewId.getEntityName());
    }
  }

  @Override
  public String getPattern() {
    return String.format("create stream-view <%s> <%s> format <%s> [schema <%s>] [settings <%s>]",
                         ArgumentName.STREAM, ArgumentName.VIEW, ArgumentName.FORMAT,
                         ArgumentName.SCHEMA, ArgumentName.SETTINGS);
  }

  @Override
  public String getDescription() {
    return String.format("Creates or updates a stream-view. Valid '<%s>'s are '%s'. '<%s>' is a SQL-like schema " +
      "'column_name data_type, ...' or an Avro-like JSON schema and '<%s>' is specified in the format " +
      "'key1=v1 key2=v2'.",
      ArgumentName.FORMAT, Joiner.on("', '").join(Formats.ALL), ArgumentName.SCHEMA, ArgumentName.SETTINGS);
  }

  @Nullable
  private Schema getSchema(Arguments arguments) throws IOException {
    if (!arguments.hasArgument(ArgumentName.SCHEMA.toString())) {
      return null;
    }

    // if it's a json object, try to parse it as json
    String schemaStr = arguments.get(ArgumentName.SCHEMA.toString());
    try {
      return Schema.parseJson(schemaStr);
    } catch (Exception e) {
      return Schema.parseSQL(schemaStr);
    }
  }
}
