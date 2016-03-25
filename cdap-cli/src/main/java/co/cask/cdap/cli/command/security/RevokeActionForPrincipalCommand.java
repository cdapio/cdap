/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.client.AuthorizationClient;
import com.google.inject.Inject;

/**
 * Revokes a principal's permission to perform certain actions on an entity.
 */
public class RevokeActionForPrincipalCommand extends RevokeActionCommand {

  @Inject
  RevokeActionForPrincipalCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(client, cliConfig);
  }

  @Override
  public String getPattern() {
    return String.format("revoke actions <actions> on entity <%s> from <%s> <%s>", ArgumentName.ENTITY,
                         ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_NAME);

  }

  @Override
  public String getDescription() {
    return "Revokes a user's permission to perform certain actions on an entity. <actions> is a comma-separated list.";
  }
}
