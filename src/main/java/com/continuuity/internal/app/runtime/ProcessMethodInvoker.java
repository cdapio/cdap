package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.InputContext;

/**
 *
 */
public interface ProcessMethodInvoker {

  void invoke(InputDatum input);
}
