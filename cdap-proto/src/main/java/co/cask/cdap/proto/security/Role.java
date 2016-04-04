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

package co.cask.cdap.proto.security;

import co.cask.cdap.api.annotation.Beta;

/**
 * Represents a {@link Role} that can be added to a {@link Principal user} or {@link Principal group}.
 * This is used in Role Based Access Control such as Apache Sentry.
 */
@Beta
public class Role extends Principal {

  public Role(String name) {
    super(name, PrincipalType.ROLE);
  }
}
