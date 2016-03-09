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
 * the License
 */

package co.cask.cdap.cli.command.metadata;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Set;

/**
 * Adds metadata tags for an entity.
 */
public class AddMetadataTagsCommand extends AbstractCommand {

  private final MetadataClient client;

  @Inject
  public AddMetadataTagsCommand(CLIConfig cliConfig, MetadataClient client) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    Set<String> tags = ImmutableSet.copyOf(parseList(arguments.get("tags")));
    client.addTags(entity.toId(), tags);
    output.println("Successfully added metadata tags");
  }

  @Override
  public String getPattern() {
    return String.format("add metadata-tags <%s> <tags>", ArgumentName.ENTITY);
  }

  @Override
  public String getDescription() {
    return "Adds metadata tags for an entity";
  }
}
