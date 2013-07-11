package com.continuuity.test.internal.bytecode;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.Flowlet;

import java.lang.annotation.Annotation;

/**
 *
 */
public final class FlowletRewriter extends AbstractProcessRewriter {

  public FlowletRewriter(String applicationId, boolean generator) {
    this(applicationId, generator ? "generate" : "process", generator ? null : ProcessInput.class);
  }

  private FlowletRewriter(String applicationId, String methodName,
                          Class<? extends Annotation> annotation) {
    super(Flowlet.class, methodName, annotation, applicationId + ".flowlet");
  }
}
