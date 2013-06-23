package com.continuuity.internal.app.services.legacy;

import com.continuuity.api.flow.flowlet.Flowlet;

/**
 * FlowletDefinition provides a read-only interface for reading attributes of
 * a single flowlet.
 */
public interface FlowletDefinition {
  /**
   * Returns the name of a flowlet.
   *
   * @return name of flowlet.
   */
  public String getName();

  /**
   * Returns the class associated with the flowlet.
   * This information is provided during programmtic configuration of a Flow.
   *
   * @return class associated with flowlet.
   */
  public Class<? extends Flowlet> getClazz();

  /**
   * Name of the class associated with flowlet.
   * This information is provided when flow is configured through flows.json.
   *
   * @return name of the class associated with flowlet.
   */
  public String getClassName();

  /**
   * Returns number of instances of a flowlet in a flow.
   *
   * @return number of instances of a flowlet.
   */
  public int getInstances();

  /**
   * Returns the resource specification associated with a flowlet.
   *
   * @return resource specification of a flowlet.
   */
  public ResourceDefinition getResources();


  /**
   * Returns the group Id associated with the flowlet.
   *
   * @return group Id associated with flowlet.
   */
  public long getGroupId();

}
