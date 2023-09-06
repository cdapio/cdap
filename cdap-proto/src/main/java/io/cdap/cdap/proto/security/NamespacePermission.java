/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

/**
 * Permissions specifically for CDAP Namespace.
 */
public enum NamespacePermission implements Permission {
  /**
   * Permission to read data in the SCM repository of a namespace.
   */
  READ_REPOSITORY,
  /**
   * Permission to write data in the SCM repository of a namespace.
   */
  WRITE_REPOSITORY,
  /**
   * Permission to update metadata of the SCM repository of a namespace.
   */
  UPDATE_REPOSITORY_METADATA,
  /**
   * Permission to set the service account associated with the namespace.
   */
  SET_SERVICE_ACCOUNT,
  /**
   * Permission to unset the service account associated with the namespace.
   */
  UNSET_SERVICE_ACCOUNT,
  /**
   * Permission to provision the credential using the service account associated with the namespace.
   */
  PROVISION_CREDENTIAL
  ;

  @Override
  public PermissionType getPermissionType() {
    return PermissionType.NAMESPACE;
  }
}
