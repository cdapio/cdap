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
package co.cask.cdap.proto.security;

import co.cask.cdap.api.annotation.Beta;

/**
 * Types of actions that users can perform on entities.
 * Actions are inherited, so granting an action on a namespace
 * would also grant that action on entities in that namespace.
 */
@Beta
public enum Action {
  /** Read data, metrics, and logs from the entity */
  READ,
  /** Write data to the entity */
  WRITE,
  /** Execute a program */
  EXECUTE,
  /** grant/revoke actions for an entity */
  ADMIN,
}
