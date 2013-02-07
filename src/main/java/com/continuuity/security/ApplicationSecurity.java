/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.security;

import java.security.Permission;

/**
 * This class is a extention of the existing java {@link SecurityManager} to allow
 * managing the permission of containers of application started by app-fabric.
 */
public final class ApplicationSecurity extends SecurityManager {
  /**
   * Collection of security permissions.
   */
  private final ApplicationPermissionCollection permissions;

  /**
   * Invoked by the builder only.
   * @param permissions a collection of {@link Permission} objects.
   */
  private ApplicationSecurity(ApplicationPermissionCollection permissions) {
    this.permissions = permissions;
  }

  /**
   * Throws a {@link SecurityException} if the requested access, specified by the given
   * <code>permission</code>, is not permitted based on the {@link Permission} objects
   * contained in the {@link ApplicationPermissionCollection}.
   * @param permission the requested permission
   */
  @Override
  public void checkPermission(final Permission permission) {
    // If there is a security manager already installed, this method first calls the security manager's
    // checkPermission method with a RuntimePermission("setSecurityManager") permission to ensure it's ok to
    // replace the existing security manager. This may result in throwing a SecurityException.
    if(permission instanceof RuntimePermission && "setSecurityManager".equals(permission.getName())) {
      throw new SecurityException("Cannot set security manager");
    }

    // For all other permissions we check by invoking implies across all the collection.
    if(permissions.implies(permission)) {
      return;
    } else {
      throw new SecurityException("Access denied to " + permission.getClass()
                                    + ", name : " + permission.getName()
                                    + ", actions : " + permission.getActions());
    }
  }

  /**
   * Static builder for constructing list of permissions.
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
    private final ApplicationPermissionCollection perms;

    /**
     * Invoked by the {@link #builder()}
     */
    private Builder() {
      perms = new ApplicationPermissionCollection();
    }

    /**
     * Adds a {@link Permission} object to the {@link ApplicationPermissionCollection}
     * @param permission the permission to be added.
     * @return this builder.
     */
    public Builder add(Permission permission) {
      perms.add(permission);
      return this;
    }

    /**
     * Applies the permission. Replaces the security manager for the JVM if there
     * none set.
     */
    public void apply() {
      perms.setReadOnly();
      SecurityManager manager = System.getSecurityManager();
      if(manager == null) {
        System.setSecurityManager(new ApplicationSecurity(perms));
      }
    }
  }
}
