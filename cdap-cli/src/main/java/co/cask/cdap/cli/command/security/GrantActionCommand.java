/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Grants a user permission to perform certain actions on an entity.
 */
public class GrantActionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  public GrantActionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    String principalName = arguments.get("principal-name");
    Principal.PrincipalType principalType = Principal.PrincipalType.valueOf(arguments.get("principal-type"));
    Principal principal = new Principal(principalName, principalType);
    Set<Action> actions = fromStrings(Splitter.on(",").split(arguments.get("actions")));

    client.grant(entity, principal, actions);
    output.printf("Successfully granted action(s) '%s' on entity '%s' for principal '%s'\n",
                  Joiner.on(",").join(actions), entity.toString(), principal);
  }

  @Override
  public String getPattern() {
    return String.format("security grant entity <%s> %s <%s> %s <%s> actions <actions>",
                         ArgumentName.ENTITY, ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_TYPE,
                         ArgumentName.PRINCIPAL_NAME, ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return "Grants a principal permission to perform certain actions on an entity. " +
      "<actions> is a comma-separated list.";
  }

  private Set<Action> fromStrings(Iterable<String> strings) {
    Set<Action> result = new HashSet<>();
    for (String string : strings) {
      result.add(Action.valueOf(string));
    }
    return result;
  }
}
