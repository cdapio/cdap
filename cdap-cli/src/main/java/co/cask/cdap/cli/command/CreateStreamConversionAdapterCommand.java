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

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Creates a stream-conversion adapter that periodically reads data from a stream and writes it
 * to a time partitioned fileset.
 */
public class CreateStreamConversionAdapterCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();

  private final AdapterClient adapterClient;

  @Inject
  public CreateStreamConversionAdapterCommand(AdapterClient adapterClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.adapterClient = adapterClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String adapterName = arguments.get(ArgumentName.ADAPTER.toString());
    String sourceName = arguments.get(ArgumentName.STREAM.toString());

    String frequency = arguments.get(ArgumentName.FREQUENCY.toString(), "10m");
    String headers = arguments.hasArgument(ArgumentName.HEADERS.toString()) ?
      arguments.get(ArgumentName.HEADERS.toString(), "") : null;
    String formatName = arguments.get(ArgumentName.FORMAT.toString(), Formats.TEXT);
    String sinkName = arguments.get(ArgumentName.DATASET.toString(), sourceName + ".converted");

    Schema sourceSchema = getSchema(arguments);
    List<Schema.Field> fields = Lists.newArrayList(sourceSchema.getFields());
    if (headers != null) {
      for (String header : headers.split(",")) {
        fields.add(Schema.Field.of(header, Schema.of(Schema.Type.STRING)));
      }
    }
    fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
    Schema sinkSchema = Schema.recordOf("event", fields);
    Map<String, String> adapterProps = Maps.newHashMap();
    adapterProps.put("frequency", frequency);
    adapterProps.put("source.name", sourceName);
    adapterProps.put("source.format.name", formatName);
    adapterProps.put("source.schema", sourceSchema.toString());
    adapterProps.put("sink.name", sinkName);
    if (headers != null) {
      adapterProps.put("headers", headers);
    }

    Map<String, String> sinkProps = FileSetProperties.builder()
      .setBasePath(sinkName)
      .setTableProperty("avro.schema.literal", sinkSchema.toString())
      .build().getProperties();

    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.type = "stream-conversion";
    adapterConfig.properties = adapterProps;
    adapterConfig.source = new AdapterConfig.Source(sourceName, Collections.<String, String>emptyMap());
    adapterConfig.sink = new AdapterConfig.Sink(sinkName, sinkProps);

    adapterClient.create(adapterName, adapterConfig);
    output.printf("Successfully created adapter named '%s' with config '%s'\n",
                  adapterName, GSON.toJson(adapterConfig));
  }

  private Schema getSchema(Arguments arguments) throws IOException {
    String schemaStr = arguments.get(ArgumentName.SCHEMA.toString(), "body string not null");
    return isJson(schemaStr) ? Schema.parseJson(schemaStr) : Schema.parseSQL(schemaStr);
  }

  private boolean isJson(String str) {
    try {
      GSON.fromJson(str, JsonObject.class);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public String getPattern() {
    return String.format("create stream-conversion adapter <%s> on <%s> [frequency <%s>]" +
                           " [format <%s>] [schema <%s>] [headers <%s>] [to <%s>]",
                         ArgumentName.ADAPTER, ArgumentName.STREAM, ArgumentName.FREQUENCY,
                         ArgumentName.FORMAT, ArgumentName.SCHEMA, ArgumentName.HEADERS,
                         ArgumentName.DATASET);
  }

  @Override
  public String getDescription() {
    return new StringBuilder()
      .append("Creates a stream conversion ")
      .append(ElementType.ADAPTER.getPrettyName())
      .append(" that periodically reads from a stream and writes to a time-partitioned fileset. ")
      .append(ArgumentName.FREQUENCY)
      .append(" is a number followed by a 'm', 'h', or 'd' for minute, hour, or day.")
      .append(ArgumentName.FORMAT)
      .append(" is the name of the stream format, such as 'text', 'avro', 'csv', or 'tsv'.")
      .append(ArgumentName.SCHEMA)
      .append(" is a sql-like schema of comma separated column name followed by column type.")
      .append(ArgumentName.HEADERS)
      .append(" is a comma separated list of stream headers to include in the output schema.")
      .append(ArgumentName.DATASET)
      .append(" is the name of the time partitioned file set to write to.")
      .toString();
  }
}
