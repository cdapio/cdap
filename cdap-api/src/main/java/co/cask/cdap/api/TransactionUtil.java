/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.api;

import co.cask.cdap.api.data.DatasetContext;
import org.apache.tephra.TransactionFailureException;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Helper class for interacting with {@link Transactional}
 * This is similar to co.cask.cdap.data2.transaction.Transactions, but added here to avoid dependency
 * on cdap-data-fabric package. Since cdap-api cannot depend on guava, some methods are copied here from Throwables.
 */
public final class TransactionUtil {

  private TransactionUtil() {
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @throws RuntimeException if failed to execute the given {@link TxRunnable} in a transaction.
   * If the TransactionFailureException has a cause in it, the cause is propagated.
   */
  public static void execute(Transactional transactional, final TxRunnable runnable) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          runnable.run(context);
        }
      });
    } catch (TransactionFailureException e) {
      throw propagate(e);
    }
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @param <X> exception type of propagate type
   * @throws X if failed to execute the given {@link TxRunnable} in a transaction. If the TransactionFailureException
   * has a cause in it, the cause is thrown as-is if it is an instance of X.
   */
  public static <X extends Throwable> void execute(Transactional transactional,
                                                   final TxRunnable runnable, Class<X> exception) throws X {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          runnable.run(context);
        }
      });
    } catch (TransactionFailureException e) {
      throw propagate(e, exception);
    }
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @return value returned by the given {@link TxCallable}.
   * @throws  RuntimeException if failed to execute the given {@link TxRunnable} in a transaction.
   * If the TransactionFailureException has a cause in it, the cause is propagated.
   */
  public static <V> V execute(Transactional transactional,
                              final TxCallable<V> callable) {
    final AtomicReference<V> result = new AtomicReference<>();
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          result.set(callable.call(context));
        }
      });
    } catch (TransactionFailureException e) {
      throw propagate(e);
    }
    return result.get();
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @return value returned by the given {@link TxCallable}.
   * @throws X if failed to execute the given {@link TxRunnable} in a transaction. If the TransactionFailureException
   * has a cause in it, the cause is thrown as-is if it is an instance of X.
   */
  public static <V, X extends Throwable> V execute(Transactional transactional,
                              final TxCallable<V> callable, Class<X> exception) throws X {
    final AtomicReference<V> result = new AtomicReference<>();
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          result.set(callable.call(context));
        }
      });
    } catch (TransactionFailureException e) {
      throw propagate(e, exception);
    }
    return result.get();
  }

  /**
   * Propagates {@code throwable} as-is if it is an instance of
   * {@link RuntimeException} or {@link Error}, or else as a last resort, wraps
   * it in a {@code RuntimeException} then propagates.
   * @param throwable the Throwable to propagate
   * @return nothing will ever be returned; this return type is only for convenience
   */
  public static RuntimeException propagate(Throwable throwable) {
    propagateIfPossible(checkNotNull(throwable));
    throw new RuntimeException(throwable);
  }

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated type if the type matches or as {@link RuntimeException}.
   * This method will always throw exception and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @param propagateType if the exception is an instance of this type, it will be rethrown as is
   * @param <X> exception type of propagate type
   * @return a exception of type X.
   */
  public static <X extends Throwable> X propagate(TransactionFailureException e,
                                                  Class<X> propagateType) throws X {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType);
    throw propagate(cause);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@link RuntimeException} or {@link Error}.
   */
  public static void propagateIfPossible(@Nullable Throwable throwable) {
    propagateIfInstanceOf(throwable, Error.class);
    propagateIfInstanceOf(throwable, RuntimeException.class);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@link RuntimeException}, {@link Error}, or
   * {@code declaredType}.
   * @param throwable the Throwable to possibly propagate
   * @param declaredType the single checked exception type declared by the calling method
   */
  public static <X extends Throwable> void propagateIfPossible(
    @Nullable Throwable throwable, Class<X> declaredType) throws X {
    propagateIfInstanceOf(throwable, declaredType);
    propagateIfPossible(throwable);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@code declaredType}.
   */
  public static <X extends Throwable> void propagateIfInstanceOf(
    @Nullable Throwable throwable, Class<X> declaredType) throws X {
    if (throwable != null && declaredType.isInstance(throwable)) {
      throw declaredType.cast(throwable);
    }
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling
   * method is not null.
   *
   * @param reference an object reference
   * @return the non-null reference that was validated
   * @throws NullPointerException if {@code reference} is null
   */
  public static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }

  /**
   * Returns the first of two given parameters that is not {@code null}, if
   * either is, or otherwise throws a {@link NullPointerException}.
   */
  public static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first != null ? first : checkNotNull(second);
  }
}
