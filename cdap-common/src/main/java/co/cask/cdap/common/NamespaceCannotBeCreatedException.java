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

package co.cask.cdap.common;

import co.cask.cdap.proto.Id;

/**
 * Thrown when a namespace cannot be created due to errors.
 */
public class NamespaceCannotBeCreatedException extends CannotBeCreatedException {

  private final Id.Namespace namespaceId;

  public NamespaceCannotBeCreatedException(Id.Namespace namespaceId, String reason) {
    super(namespaceId, reason);
    this.namespaceId = namespaceId;
  }

  public NamespaceCannotBeCreatedException(Id.Namespace namespaceId, Throwable cause) {
    super(namespaceId, cause);
    this.namespaceId = namespaceId;
  }

  public Id.Namespace getNamespaceId() {
    return namespaceId;
  }
}
