/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 *
 */
public abstract class AbstractFlowlet implements Flowlet {

  protected FlowletContext flowletContext;

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.builder()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    this.flowletContext = context;
  }

  @Override
  public void destroy() {
    // Nothing to do.
  }

  protected String getName() {
    return getClass().getSimpleName();
  }

  protected String getDescription() {
    return String.format("Flowlet for doing %s.", getName());
  }
}
