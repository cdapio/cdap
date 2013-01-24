package com.continuuity.passport.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines Components in the system.
 */
public class Component {

  public enum ComponentType { DATASET, VPC}
  private String name;
  private ComponentType componentType;
  private String id;
  private Set<ComponentACL> acls = new HashSet<ComponentACL>();

  public void setComponentType(ComponentType c) {
    this.componentType = componentType;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Set<ComponentACL> getComponentACLs() {
    return Collections.unmodifiableSet(this.acls);
  }

  public void addComponentAcl(ComponentACL acl) {
    this.acls.add(acl);

  }

}
