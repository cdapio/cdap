/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;

import java.util.concurrent.Executor;

/**
 *
 */
public interface PostProcess {
  
  public interface InputAcknowledger {
    void ack() throws OperationException;
  }

  public interface Callback {
    void onSuccess(Object inputObject, InputContext inputContext);

    void onFailure(Object inputObject, InputContext inputContext,
                   FailureReason reason, InputAcknowledger inputAcknowledger);
  }

  /**
   * Commits the transaction using the given executor. Result of the commit would be
   * reflected by invoking callback methods.
   *
   * @param executor
   * @param callback
   */
  void commit(Executor executor, Callback callback);
}
