/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.api.annotation.Beta;

/**
 * A Enum which can represent categorizes different types of operations which are performed with impersonation.
 * Currently, explore queries running inside a namespace is categorized as {@link #EXPLORE} and all other operations
 * as {@link #OTHER}.
 */
@Beta
public enum ImpersonatedOpType {
  EXPLORE,
  OTHER
}
