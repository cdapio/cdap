package com.continuuity.api.flow.flowlet;

/**
 *
 */
public interface Flowlet {

  FlowletSpecification configure();

  void initialize(FlowletContext context) throws FlowletException;

  void destroy();
}
