/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;

/**
 * Interface to represent callback object for commit result.
 */
interface ProcessMethodCallback {
  void onSuccess(Object inputObject, InputContext inputContext);

  void onFailure(Object inputObject, InputContext inputContext,
                 FailureReason reason, InputAcknowledger inputAcknowledger);
}
