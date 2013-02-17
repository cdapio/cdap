package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.InputContext;
import com.google.common.base.Throwables;

import java.lang.reflect.Method;

/**
 * A {@link TransactionCallback} that uses reflection to invoke.
 */
public final class ReflectionTransactionCallback implements TransactionCallback {

  private final Object target;
  private final Method success;
  private final Method failure;

  public ReflectionTransactionCallback(Object target, Method success, Method failure) {
    this.target = target;
    this.success = success;
    this.failure = failure;

    if(!this.success.isAccessible()) {
      this.success.setAccessible(true);
    }
    if(!this.failure.isAccessible()) {
      this.failure.setAccessible(true);
    }
  }

  @Override
  public void onSuccess(Object object, InputContext inputContext) {
    try {
      success.invoke(target, object, inputContext);
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public FailurePolicy onFailure(Object object, InputContext inputContext) {
    try {
      return (FailurePolicy) failure.invoke(target, object, inputContext);
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
