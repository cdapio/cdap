/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
 * the License
 */

package io.cdap.cdap.cli.command.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Removes metadata for an entity.
 */
public class RemoveMetadataCommand extends AbstractCommand {

  private final MetadataClient client;

  @Inject
  public RemoveMetadataCommand(CLIConfig cliConfig, MetadataClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    MetadataEntity metadataEntity =
      MetadataCommandHelper.toMetadataEntity(arguments.get(ArgumentName.ENTITY.toString()));
    client.removeMetadata(metadataEntity);
    output.println("Successfully removed metadata");
  }

  @Override
  public String getPattern() {
    return String.format("remove metadata <%s>", ArgumentName.ENTITY);
  }

  @Override
  public String getDescription() {
    return "Removes metadata for an entity. " + ArgumentName.ENTITY_DESCRIPTION_STRING;
  }
}
