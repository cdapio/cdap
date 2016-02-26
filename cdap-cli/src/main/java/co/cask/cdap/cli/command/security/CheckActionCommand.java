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
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Checks whether a user has permission to perform the specified action on an entity.
 */
public class CheckActionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  public CheckActionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    EntityId entity = EntityId.fromString(arguments.get(ArgumentName.ENTITY.toString()));
    String principalName = arguments.get("principal-name");
    Principal.PrincipalType principalType = Principal.PrincipalType.valueOf(arguments.get("principal-type"));
    Principal principal = new Principal(principalName, principalType);
    Action action = Action.valueOf(arguments.get("action"));

    try {
      client.authorized(entity, principal, action);
      output.printf("Principal %s is authorized to perform action %s on entity %s\n", principal, action, entity);
    } catch (UnauthorizedException e) {
      output.printf("Principal %s is not authorized to perform action %s on entity %s\n", principal, action, entity);
    }
  }

  @Override
  public String getPattern() {
    return String.format("security access entity <%s> %s <%s> %s <%s> action <action>",
                         ArgumentName.ENTITY, ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_TYPE,
                         ArgumentName.PRINCIPAL_NAME, ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return "Checks whether a principal is authorized to perform an action on an entity.";
  }
}
