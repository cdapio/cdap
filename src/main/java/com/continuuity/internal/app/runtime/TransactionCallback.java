package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;

import javax.annotation.Nullable;

/**
 *
 */
public interface TransactionCallback {

  void onSuccess(@Nullable Object object, @Nullable InputContext inputContext);

  FailurePolicy onFailure(@Nullable Object object, @Nullable InputContext inputContext, FailureReason reason);
}
