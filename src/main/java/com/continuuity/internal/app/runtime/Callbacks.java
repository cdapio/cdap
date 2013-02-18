package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;

/**
 * Factory class for creating {@link com.continuuity.api.flow.flowlet.Callback}.
 */
public final class Callbacks {

  /**
   * Same as call {@link #createFrom(Object, com.continuuity.api.flow.flowlet.FailurePolicy)} with the
   * {@link FailurePolicy#RETRY} as the default failure policy.
   */
  public static Callback createFrom(Object target) {
    return createFrom(target, FailurePolicy.RETRY);
  }

  /**
   * Returns an instance of {@link Callback} that would delegates the calls to the given object
   * if the given object implements {@link Callback}. Otherwise, it would returns a
   * {@link Callback} that do nothing onSuccess and return the given {@code defaultFailurePolicy}
   * from the onFailure method.
   *
   * @param target The instance to inspect for callback methods.
   * @return A {@link Callback}.
   */
  public static Callback createFrom(Object target, final FailurePolicy defaultFailurePolicy) {
    if (target instanceof Callback) {
      return (Callback)target;
    }

    return new Callback() {
      @Override
      public void onSuccess(Object input, InputContext inputContext) {
        // No-op
      }

      @Override
      public FailurePolicy onFailure(Object input, InputContext inputContext, FailureReason reason) {
        return defaultFailurePolicy;
      }
    };
  }

  private Callbacks() {
  }
}
