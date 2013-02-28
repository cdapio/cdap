package com.continuuity.internal.test.bytecode;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.Flowlet;

/**
 *
 */
public final class FlowletRewriter extends AbstractProcessRewriter {

  public FlowletRewriter(String applicationId) {
    super(Flowlet.class, "process", ProcessInput.class, applicationId + ".flowlet");
  }
}
