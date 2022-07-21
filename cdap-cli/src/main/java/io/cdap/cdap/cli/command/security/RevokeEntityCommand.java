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

package io.cdap.cdap.cli.command.security;

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.client.AuthorizationClient;

/**
 * Revokes all permissions for all principal on an entity
 */
public class RevokeEntityCommand extends RevokePermissionCommand {

  @Inject
  RevokeEntityCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(client, cliConfig);
  }

  @Override
  public String getPattern() {
    return String.format("revoke all on entity <%s>", ArgumentName.ENTITY);
  }

  @Override
  public String getDescription() {
    return "Revokes all privileges for all users on the entity. " + ArgumentName.ENTITY_DESCRIPTION_ALL_STRING;
  }
}
