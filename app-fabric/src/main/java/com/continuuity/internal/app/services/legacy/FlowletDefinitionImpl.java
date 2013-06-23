package com.continuuity.internal.app.services.legacy;

import com.continuuity.api.flow.flowlet.Flowlet;

/**
 * Implementation of FlowletDefinition.
 * FlowletDefinitionImpl implements two interfaces namely FlowletDefinition and
 * FlowletDefinitionModifier. One is a read-only interface and other is not.
 */
public class FlowletDefinitionImpl implements FlowletDefinition, FlowletDefinitionModifier {
  /**
   * Name of this flowlet.
   */
  private String name;

  /**
   * Class that implements this flowlet.
   * clazz is transient to make sure that it is not serialized.
   */
  private transient Class<? extends Flowlet> clazz;

  /**
   * Class name associated with the flowlet.
   */
  private String classname;

  /**
   * Number of instances of this flowlet in a flow.
   */
  private int instances;

  /**
   * Resource specification of this flowlet.
   */
  private ResourceDefinitionImpl resource;

  /**
   * ID of the flowlet.
   */
  private int id;

  /**
   * Type of this flowlet.
   */
  private String type;

  /**
   * Group Id associated with the flowlet.
   */
  private long groupId;

  /**
   * Creates an default instance of a FlowletDefinitionImpl.
   */
  public FlowletDefinitionImpl() {
    this.instances = 1;
    this.clazz = null;
    this.resource = null;
    this.classname = null;
  }

  /**
   * Returns the name of a flowlet.
   *
   * @return name of flowlet.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Sets a new name of this flowlet.
   *
   * @param name of this flowlet.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the class associated with the flowlet.
   *
   * This information is provided during programmtic configuration of a Flow.
   *
   * @return class associated with flowlet.
   */
  @Override
  public Class<? extends Flowlet> getClazz() {
    return clazz;
  }

  /**
   * Set a new flowlet class that provides an implementation for this flowlet.
   *
   * @param clazz flowlet class to be associated with this flowlet.
   */
  public void setClazz(Class<? extends Flowlet> clazz) {
    this.clazz = clazz;
  }

  /**
   * Sets the type of flowlet in the flowlet definition.
   *
   * @param type of the flowlet.
   */
  @Override
  public void setFlowletType(FlowletType type) {
    this.type = type.getName();
  }

  /**
   * Sets the groupId.
   *
   * @param groupId associated with flowlet.
   */
  @Override
  public void setGroupId(long groupId) {
    this.groupId = groupId;
  }

  /**
   * Name of the class associated with flowlet.
   * This information is provided when flow is configured through flows.json.
   *
   * @return name of the class associated with flowlet.
   */
  @Override
  public String getClassName() {
    return classname;
  }

  /**
   * Sets a new classname
   * @param classname name of the class to be associated with this flowlet.
   */
  public void setClassName(String classname) {
    this.classname = classname;
  }

  /**
   * Returns number of instances of a flowlet in a flow.
   *
   * @return number of instances of a flowlet.
   */
  @Override
  public int getInstances() {
    return instances;
  }

  /**
   * Specifies new instance count for a flowlet.
   *
   * @param newInstances number of new instances of a flowlet
   * @return old instance count.
   */
  @Override
  public int setInstances(int newInstances) {
    int oldInstances = instances;
    instances = newInstances;
    return oldInstances;
  }

  /**
   * Returns the resource specification associated with a flowlet.
   *
   * @return resource specification of a flowlet.
   */
  @Override
  public ResourceDefinition getResources() {
    return resource;
  }

  /**
   * Returns the group Id associated with the flowlet.
   *
   * @return group Id associated with flowlet.
   */
  @Override
  public long getGroupId() {
    return groupId;
  }

  /**
   * Sets a new resource specification for this flowlet.
   *
   * @param resource new specification of resources for this flowlet.
   */
  public void setResources(ResourceDefinitionImpl resource) {
    this.resource = resource;
  }

}
