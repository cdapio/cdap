/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * Defines Components in the system.
 */
public class Component {

  private final String name;

  private final String componentType;

  private final String id;

  private final Set<ComponentACL> acls;

  public Component(final String name, final String id, final String type, final Set<ComponentACL> acls) {
    this.name = name;
    this.id = id;
    this.componentType = type;
    this.acls = acls;
  }

  public Component(final String name, final String id, final String type) {
    this.name = name;
    this.id = id;
    this.componentType = type;
    this.acls = new HashSet<ComponentACL>();
  }

  public String getName() {
    return name;
  }

  public String getComponentType() {
    return componentType;
  }

  public String getId() {
    return id;
  }

  /**
   * getACLs for the component.
   * @return ImmutableSet of ComponentACL
   */
  public Set<ComponentACL> getComponentACLs() {
    return ImmutableSet.copyOf(this.acls);
  }


}
