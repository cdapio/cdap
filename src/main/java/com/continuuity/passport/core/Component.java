package com.continuuity.passport.core;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines Components in the system.
 */
public class Component {

  public enum Type { DATASET, VPC}
  private String name;
  private Type componentType;
  private String id;
  private Set<ComponentACL> acls = new HashSet<ComponentACL>();

  public Component(String name, String id, Type type){
    this.name = name;
    this.id = id;
    this.componentType = type;
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
   * @return ImmutableSet of ComponentACL
   */
  public ImmutableSet<ComponentACL> getComponentACLs() {
    return ImmutableSet.copyOf(this.acls);
  }

  public void addComponentAcl(ComponentACL acl) {
    this.acls.add(acl);

  }

}
