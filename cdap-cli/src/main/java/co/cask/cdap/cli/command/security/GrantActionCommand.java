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
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Set;

/**
 * Grants a user permission to perform certain actions on an auhorizable.
 */
public class GrantActionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  GrantActionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Authorizable authorizable = new Authorizable(arguments.get(ArgumentName.ENTITY.toString()));
    String principalName = arguments.get("principal-name");
    Principal.PrincipalType principalType =
      Principal.PrincipalType.valueOf(arguments.get("principal-type").toUpperCase());
    Principal principal = new Principal(principalName, principalType);
    Set<Action> actions = ACTIONS_STRING_TO_SET.apply(arguments.get("actions"));
    // actions is not an optional argument so should never be null
    Preconditions.checkNotNull(actions, "Actions can never be null in the grant command.");

    client.grant(authorizable, principal, actions);
    output.printf("Successfully granted action(s) '%s' on entity '%s' to %s '%s'\n",
                  Joiner.on(",").join(actions), authorizable.toString(), principal.getType(), principal.getName());
  }

  @Override
  public String getPattern() {
    return String.format("grant actions <actions> on entity <%s> to <%s> <%s>", ArgumentName.ENTITY,
                         ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return String.format("Grants a principal privileges to perform certain actions on an authorizable. %s %s",
      ArgumentName.ENTITY_DESCRIPTION_ACTIONS, ArgumentName.ENTITY_DESCRIPTION_ALL_STRING);
  }
}
