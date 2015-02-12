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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;

/**
 * Sets the Format Specification of a stream.
 */
public class SetStreamFormatCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();
  private final StreamClient streamClient;
  private final String namespace;

  @Inject
  public SetStreamFormatCommand(StreamClient streamClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
    this.namespace = cliConfig.getCurrentNamespace();
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String streamName = arguments.get(ArgumentName.STREAM.toString());
    Id.Stream streamId = Id.Stream.from(namespace, streamName);
    StreamProperties currentProperties = streamClient.getConfig(streamName);

    String formatName = arguments.get(ArgumentName.FORMAT.toString());
    Schema schema = getSchema(arguments);
    Map<String, String> settings = Collections.emptyMap();
    if (arguments.hasArgument(ArgumentName.SETTINGS.toString())) {
      settings = Splitter.on(" ").withKeyValueSeparator("=").split(arguments.get(ArgumentName.SETTINGS.toString()));
    }
    FormatSpecification formatSpecification = new FormatSpecification(formatName, schema, settings);
    StreamProperties streamProperties = new StreamProperties(streamId, currentProperties.getTTL(),
                                                             formatSpecification, currentProperties.getThreshold());
    streamClient.setStreamProperties(streamName, streamProperties);
    output.printf("Successfully set format of stream '%s'\n", streamName);
  }

  private Schema getSchema(Arguments arguments) throws IOException {
    Schema schema = null;
    if (arguments.hasArgument(ArgumentName.SCHEMA.toString())) {
      // if it's a json object, try to parse it as json
      String schemaStr = arguments.get(ArgumentName.SCHEMA.toString());
      schema = isJsonObject(schemaStr) ? Schema.parseJson(schemaStr) : Schema.parseSQL(schemaStr);
    }
    return schema;
  }

  private boolean isJsonObject(String str) {
    try {
      GSON.fromJson(str, JsonObject.class);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public String getPattern() {
    return String.format("set stream format <%s> <%s> [<%s>] [<%s>]", ArgumentName.STREAM, ArgumentName.FORMAT,
                         ArgumentName.SCHEMA, ArgumentName.SETTINGS);
  }

  @Override
  public String getDescription() {
    return new StringBuilder()
      .append("Sets the format of a ")
      .append(ElementType.STREAM.getPrettyName())
      .append(". <")
      .append(ArgumentName.SCHEMA)
      .append("> is a sql-like schema \"column_name data_type, ...\" or avro-like json schema and <")
      .append(ArgumentName.SETTINGS)
      .append("> is specified in the format \"key1=v1, key2=v2\"")
      .toString();
  }
}
