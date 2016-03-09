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

package co.cask.cdap.cli.command.security;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.RevokeRequest;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Revokes a user's permission to perform certain actions on an entity.
 */
public class RevokeActionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  public RevokeActionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    String user = arguments.getOptional("user", null);
    String actionsString = arguments.getOptional("actions", null);
    Set<Action> actions = actionsString == null ? null : fromStrings(Splitter.on(",").split(actionsString));

    client.revoke(new RevokeRequest(entity, user, actions));
    if (user == null) {
      output.printf("Successfully revoked all actions on entity '%s' for all users", entity.toString());
    } else if (actions == null) {
      output.printf("Successfully revoked all actions on entity '%s' for user '%s'\n",
                    entity.toString(), user);
    } else {
      output.printf("Successfully revoked action(s) '%s' on entity '%s' for user '%s'\n",
                    Joiner.on(",").join(actions), entity.toString(), user);
    }
  }

  @Override
  public String getPattern() {
    return String.format("security revoke entity <%s> [user <user>] [actions <actions>]", ArgumentName.ENTITY);
  }

  @Override
  public String getDescription() {
    return "Revokes a user's permission to perform certain actions on an entity. <actions> is a comma-separated list.";
  }

  private Set<Action> fromStrings(Iterable<String> strings) {
    Set<Action> result = new HashSet<>();
    for (String string : strings) {
      result.add(Action.valueOf(string));
    }
    return result;
  }
}
