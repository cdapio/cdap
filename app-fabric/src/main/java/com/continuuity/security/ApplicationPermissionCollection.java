/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.security;

import com.google.common.collect.Lists;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents a collection of {@link Permission} objects.
 * <p>
 * With a {@link ApplicationPermissionCollection}, you can:
 * <ul>
 * <li>add a permission to the collection using the <code>add</code> method</li>
 * <li>check to see if a particular permission is implied in the collection, using the <code>implies</code> method</li>
 * <li>enumerate all the permission, using the <code>elements</code> method</li>
 * </ul>
 * </p>
 * <p/>
 * <p>
 * This collection does not group the permissions, but these are collection of the permission
 * related to an application that we want to restrict.
 * </p>
 */
class ApplicationPermissionCollection extends PermissionCollection {
  private final List<Permission> perms = Lists.newArrayList();

  /**
   * Constructor that defines some predefined {@link Permission}
   */
  public ApplicationPermissionCollection() {
    perms.add(new RuntimePermission("getenv.*"));
    perms.add(new RuntimePermission("setContextClassLoader"));
  }

  /**
   * Adds a {@link Permission} object to the current collection of permission objects.
   *
   * @param p the {@link Permission} object to be added.
   */
  public void add(Permission p) {
    synchronized (perms) {
      perms.add(p);
    }
  }

  /**
   * Checks to see if the specified {@link Permission} <code>p</code> is implied by the collection
   * of {@link Permission} objects held by the {@link ApplicationPermissionCollection}
   *
   * @param p the {@link Permission} object to compare.
   * @return true if "permission" is implied by the permissions in the collection, false if not.
   */
  public boolean implies(Permission p) {
    Iterator<Permission> i = perms.iterator();
    while (i.hasNext()) {
      Permission p1 = ((Permission) i.next());
      if (p.getClass().isAssignableFrom(p1.getClass()) && p1.implies(p)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return an enumeration of all {@link Permission}s
   */
  @Override
  public Enumeration<Permission> elements() {
    return Collections.enumeration(perms);
  }
}
