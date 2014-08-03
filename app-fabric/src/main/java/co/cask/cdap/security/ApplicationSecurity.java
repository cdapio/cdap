/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.security;

import java.security.Permission;
import java.security.PermissionCollection;

/**
 * This class is a extention of the existing java {@link SecurityManager} to allow
 * managing the permission of containers of application started by app-fabric.
 */
public final class ApplicationSecurity extends SecurityManager {
  /**
   * Collection of security permissions.
   */
  private final PermissionCollection permissions;

  /**
   * Admin class as set that has ability to by-pass the security manager.
   */
  private final Class<?> adminClass;

  /**
   * Invoked by the builder only.
   *
   * @param permissions a collection of {@link Permission} objects.
   * @param adminClass  Class that had administratives
   */
  private ApplicationSecurity(PermissionCollection permissions, Class<?> adminClass) {
    this.permissions = permissions;
    this.adminClass = adminClass;
  }

  /**
   * Throws a {@link SecurityException} if the requested access, specified by the given
   * <code>permission</code>, is not permitted based on the {@link Permission} objects
   * contained in the {@link ApplicationPermissionCollection}.
   *
   * @param permission the requested permission
   */
  @Override
  public void checkPermission(final Permission permission) {
    // We get the current execution stack as an array of classes.
    Class<?>[] klasses = getClassContext();
    boolean isAdminClass = false;

    // Iterate and see if the stack has a call that's coming from
    // the adminClass. If it is, then consider this check coming
    // the admin class.
    if (adminClass != null) {
      for (Class<?> k : klasses) {
        if (k == this.adminClass) {
          isAdminClass = true;
          break;
        }
      }
    }

    // If admin found on stack, we return, that class is given admin rights.
    if (isAdminClass) {
      return;
    }

    // If there is a security manager already installed, this method first calls the security manager's
    // checkPermission method with a RuntimePermission("setSecurityManager") permission to ensure it's ok to
    // replace the existing security manager. This may result in throwing a SecurityException.
    if (permission instanceof RuntimePermission && "setSecurityManager".equals(permission.getName())) {
      throw new SecurityException("Cannot set security manager");
    }

    // For all other permissions we check by invoking implies across all the collection.
    if (permissions.implies(permission)) {
      return;
    } else {
      throw new SecurityException(
                                   "Access denied to " + permission.getClass()
                                     + ", name : " + permission.getName()
                                     + ", actions : " + permission.getActions()
      );
    }
  }

  /**
   * Static builder for constructing list of permissions.
   *
   * @return An instance of builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for constructing the permissions collection.
   * The permission collection is a allowed permission list.
   */
  public static class Builder {
    private final PermissionCollection perms;
    private Class<?> klass = null;

    /**
     * Invoked by the {@link #builder()}.
     */
    private Builder() {
      perms = new ApplicationPermissionCollection();
    }

    /**
     * Adds a {@link Permission} object to the {@link ApplicationPermissionCollection}.
     *
     * @param permission the permission to be added.
     * @return this builder.
     */
    public Builder add(Permission permission) {
      perms.add(permission);
      return this;
    }

    /**
     * Defines the admin class that has capability to operate outside of restriction
     * as set by {@link ApplicationSecurity}.
     *
     * @param klass that has admin rights within this security manager.
     * @return this builder.
     */
    public Builder adminClass(Class<?> klass) {
      this.klass = klass;
      return this;
    }

    /**
     * Applies the permission. Replaces the security manager for the JVM if there
     * none set.
     */
    public void apply() {
      System.setSecurityManager(new ApplicationSecurity(perms, klass));
    }
  }
}
