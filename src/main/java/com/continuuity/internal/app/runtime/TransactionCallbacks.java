package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * Factory class for creating {@link TransactionCallback}.
 */
public final class TransactionCallbacks {

  /**
   * Same as call {@link #createFrom(Object, com.continuuity.api.flow.flowlet.FailurePolicy)} with the
   * {@link FailurePolicy#RETRY} as the default failure policy.
   */
  public static TransactionCallback createFrom(Object target) {
    return createFrom(target, FailurePolicy.RETRY);
  }

  /**
   * Returns an instance of {@link TransactionCallback} that would delegates the calls to the given object
   * if the given object contains the onSuccess and onFailure method. Otherwise, it would returns a
   * {@link TransactionCallback} that do nothing onSuccess and return the given {@code defaultFailurePolicy}
   * from the onFailure method.
   *
   * @param target The instance to inspect for callback methods.
   * @return A {@link TransactionCallback}.
   */
  public static TransactionCallback createFrom(Object target, final FailurePolicy defaultFailurePolicy) {
    TypeToken<?> targetType = TypeToken.of(target.getClass());

    // Inspect all methods.
    Method successMethod = null;
    Method failureMethod = null;
    for(TypeToken<?> type : targetType.getTypes().classes()) {
      for(Method method : type.getRawType().getDeclaredMethods()) {
        for(Method callbackMethod : TransactionCallback.class.getMethods()) {
          if(!compareMethod(callbackMethod, method)) {
            continue;
          }

          if(successMethod == null && void.class.equals(method.getReturnType())) {
            successMethod = method;
          } else if(failureMethod == null) {
            failureMethod = method;
          }
        }
        if(successMethod != null && failureMethod != null) {
          return new ReflectionTransactionCallback(target, successMethod, failureMethod);
        }
      }
    }

    return new TransactionCallback() {
      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        // No-op
      }

      @Override
      public FailurePolicy onFailure(Object object, InputContext inputContext, FailureReason reason) {
        return defaultFailurePolicy;
      }
    };
  }

  /**
   * Compare two methods to see if they have the same name, same return type and same parameters
   *
   * @param first
   * @param second
   * @return {@code true} if they are the same, {@code false} otherwise.
   */
  private static boolean compareMethod(Method first, Method second) {
    if(!first.getName().equals(second.getName())) {
      return false;
    }

    if(!first.getGenericReturnType().equals(second.getGenericReturnType())) {
      return false;
    }

    Type[] firstParams = first.getGenericParameterTypes();
    Type[] secondParams = second.getGenericParameterTypes();

    if(firstParams.length != secondParams.length) {
      return false;
    }

    for(int i = 0; i < firstParams.length; i++) {
      if(!firstParams[i].equals(secondParams[i])) {
        return false;
      }
    }
    return true;
  }

  private TransactionCallbacks() {
  }
}
