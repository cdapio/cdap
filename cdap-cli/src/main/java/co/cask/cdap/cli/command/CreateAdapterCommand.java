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
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.common.cli.Arguments;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Creates an adapter.
 */
public class CreateAdapterCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();

  private final AdapterClient adapterClient;

  @Inject
  public CreateAdapterCommand(AdapterClient adapterClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.adapterClient = adapterClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Map<String, String> sourceProps = GSON.fromJson(arguments.get(ArgumentName.ADAPTER_SOURCE_PROPS.toString(), "{}"),
                                                    new TypeToken<Map<String, String>>() { }.getType());
    Map<String, String> sinkProps = GSON.fromJson(arguments.get(ArgumentName.ADAPTER_SINK_PROPS.toString(), "{}"),
                                                  new TypeToken<Map<String, String>>() { }.getType());
    Map<String, String> adapterProps = GSON.fromJson(arguments.get(ArgumentName.ADAPTER_PROPS.toString(), "{}"),
                                                     new TypeToken<Map<String, String>>() { }.getType());

    String adapterName = arguments.get(ArgumentName.ADAPTER.toString());
    AdapterConfig adapterConfig = new AdapterConfig();
    adapterConfig.type = arguments.get(ArgumentName.ADAPTER_TYPE.toString());
    adapterConfig.properties = adapterProps;
    adapterConfig.source = new AdapterConfig.Source(
      arguments.get(ArgumentName.ADAPTER_SOURCE.toString()), sourceProps);
    adapterConfig.sink = new AdapterConfig.Sink(
      arguments.get(ArgumentName.ADAPTER_SINK.toString()), sinkProps);

    adapterClient.create(adapterName, adapterConfig);
    output.printf("Successfully created adapter named '%s' with config '%s'\n",
                  adapterName, GSON.toJson(adapterConfig));
  }

  @Override
  public String getPattern() {
    return String.format("create adapter <%s> type <%s> [props <%s>]" +
                         " src <%s> [src-props <%s>] sink <%s> [sink-props <%s>]",
                         ArgumentName.ADAPTER, ArgumentName.ADAPTER_TYPE,
                         ArgumentName.ADAPTER_PROPS,
                         ArgumentName.ADAPTER_SOURCE, ArgumentName.ADAPTER_SOURCE_PROPS,
                         ArgumentName.ADAPTER_SINK, ArgumentName.ADAPTER_SINK_PROPS);
  }

  @Override
  public String getDescription() {
    return String.format("Creates a %s.", ElementType.ADAPTER.getPrettyName());
  }
}
