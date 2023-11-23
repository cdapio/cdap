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

package io.cdap.cdap.security.authorization;

import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A helper SINGLETON class to store privileges.
 *
 * So this can act like single in-memory storage for AccessControllers , RoleControllers and Permission Managers
 */
public class InMemoryPrivilegeHolder {
  private static InMemoryPrivilegeHolder instance;
  protected final static ConcurrentMap<Authorizable, ConcurrentMap<Principal, Set<Permission>>> privileges =
    new ConcurrentHashMap<>();
  protected final static ConcurrentMap<Role, Set<Principal>> roleToPrincipals = new ConcurrentHashMap<>();

  private InMemoryPrivilegeHolder() {}

  public static synchronized InMemoryPrivilegeHolder getInstance() {
    if (instance == null) {
      instance = new InMemoryPrivilegeHolder();
    }
    return instance;
  }

  public static ConcurrentMap<Authorizable, ConcurrentMap<Principal, Set<Permission>>> getPrivileges() {
    if (instance == null) {
      getInstance();
    }
    return privileges;
  }

  public static ConcurrentMap<Role, Set<Principal>> getRoleToPrincipals() {
    if (instance == null) {
      getInstance();
    }
    return roleToPrincipals;
  }
}
