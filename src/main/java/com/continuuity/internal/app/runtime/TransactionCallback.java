package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.InputContext;

/**
 *
 */
public interface TransactionCallback {

  void onSuccess(Object object, InputContext inputContext);

  FailurePolicy onFailure(Object object, InputContext inputContext);
}
