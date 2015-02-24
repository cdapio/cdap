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

package co.cask.cdap.internal.app.namespace;

/**
 * Thrown when a namespace cannot be created due to errors.
 * TODO: Make generic and move to common if its needed elsewhere
 */
public class NamespaceCannotBeCreatedException extends Exception {

  private final String namespaceId;
  private final String reason;

  public NamespaceCannotBeCreatedException(String namespaceId, String reason) {
    super(String.format("Namespace %s cannot be created. Reason: %s", namespaceId, reason));
    this.namespaceId = namespaceId;
    this.reason = reason;
  }

  public NamespaceCannotBeCreatedException(String namespaceId, Throwable cause) {
    super(String.format("Namespace %s cannot be created. Reason: %s", namespaceId, cause.getMessage()), cause);
    this.namespaceId = namespaceId;
    this.reason = cause.getMessage();
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getReason() {
    return reason;
  }
}
