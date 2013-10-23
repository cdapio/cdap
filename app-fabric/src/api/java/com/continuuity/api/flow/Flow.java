/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.Processor;

/**
 * Flow is a collection of {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} that are
 * wired together into a Direct Acylic Graph (DAG).
 *
 * <p>
 *   Implement this interface to create a flow. The {@link #configure()} method will be
 *   invoked during deployment time and it returns a {@link FlowSpecification} to specify how to
 *   configure the flow.
 * </p>
 *
 * See the <i>Continuuity Reactor Developer Guide</i> and the Reactor example applications.
 * @see FlowSpecification
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 */
public interface Flow extends Processor {
  /**
   * Configure the {@link Flow} by returning a {@link FlowSpecification}.
   *
   * @return An instance of {@link FlowSpecification}.
   */
  FlowSpecification configure();
}
