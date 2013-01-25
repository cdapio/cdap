package com.continuuity.passport.core;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * Defines Components in the system.
 */
public class Component {

  public enum Type {DATASET, VPC}

  private final  String name;

  private final  Type componentType;

  private final String id;

  private final Set<ComponentACL> acls;

  public Component( final String name,  final String id,  final Type type,  final Set<ComponentACL> acls) {
    this.name = name;
    this.id = id;
    this.componentType = type;
    this.acls = acls;
  }

  public Component( final String name,  final String id,  final Type type) {
    this.name = name;
    this.id = id;
    this.componentType = type;
    this.acls = new HashSet<ComponentACL>();
  }

  public String getName() {
    return name;
  }

  public Type getComponentType() {
    return componentType;
  }

  public String getId() {
    return id;
  }

  /**
   * getACLs for the component
   *
   * @return ImmutableSet of ComponentACL
   */
  public Set<ComponentACL> getComponentACLs() {
    return ImmutableSet.copyOf(this.acls);
  }


}
