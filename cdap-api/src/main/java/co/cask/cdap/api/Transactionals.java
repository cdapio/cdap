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

import org.apache.tephra.TransactionFailureException;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for interacting with {@link Transactional}
 */
public final class Transactionals {

  private Transactionals() {
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @throws RuntimeException if failed to execute the given {@link TxRunnable} in a transaction.
   * If the TransactionFailureException has a cause in it, the cause is propagated.
   */
  public static void execute(Transactional transactional, TxRunnable runnable) {
    try {
      transactional.execute(runnable);
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
   * @throws RuntimeException if cause is not an instance of X. The cause is wrapped with {@link RuntimeException}
   * if it is not already a {@link RuntimeException}.
   */
  public static <X extends Throwable> void execute(Transactional transactional,
                                                   TxRunnable runnable, Class<X> exception) throws X {
    try {
      transactional.execute(runnable);
    } catch (TransactionFailureException e) {
      throw propagate(e, exception);
    }
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @throws X1 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionFailureException has a cause in it, the cause is thrown as-is if it is an instance of X1.
   * @throws X2 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionFailureException has a cause in it, the cause is thrown as-is if it is an instance of X2.
   * @throws RuntimeException if cause is not an instance of X1 or X2. The cause is wrapped with
   * {@link RuntimeException} if it is not already a {@link RuntimeException}.
   */
  public static <X1 extends Throwable, X2 extends Throwable> void execute(Transactional transactional,
                                                                          TxRunnable runnable,
                                                                          Class<X1> exception1,
                                                                          Class<X2> exception2) throws X1, X2 {
    try {
      transactional.execute(runnable);
    } catch (TransactionFailureException e) {
      throw propagate(e, exception1, exception2);
    }
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link Transactional}.
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @return value returned by the given {@link TxCallable}.
   * @throws  RuntimeException if failed to execute the given {@link TxRunnable} in a transaction.
   * If the TransactionFailureException has a cause in it, the cause is propagated.
   */
  public static <V> V execute(Transactional transactional, TxCallable<V> callable) {
    AtomicReference<V> result = new AtomicReference<>();
    try {
      transactional.execute(context -> result.set(callable.call(context)));
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
   * @param <X> exception type of propagate type
   * @return value returned by the given {@link TxCallable}.
   * @throws X if failed to execute the given {@link TxRunnable} in a transaction. If the TransactionFailureException
   * has a cause in it, the cause is thrown as-is if it is an instance of X.
   * @throws RuntimeException if cause is not an instance of X. The cause is wrapped with {@link RuntimeException}
   * if it is not already a {@link RuntimeException}.
   */
  public static <V, X extends Throwable> V execute(Transactional transactional,
                                                   TxCallable<V> callable, Class<X> exception) throws X {
    AtomicReference<V> result = new AtomicReference<>();
    try {
      transactional.execute(context -> result.set(callable.call(context)));
    } catch (TransactionFailureException e) {
      throw propagate(e, exception);
    }
    return result.get();
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link Transactional}.
   *
   * @param transactional the {@link Transactional} to use for transactional execution.
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @return value returned by the given {@link TxCallable}.
   * @throws X1 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionFailureException has a cause in it, the cause is thrown as-is if it is an instance of X1.
   * @throws X2 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionFailureException has a cause in it, the cause is thrown as-is if it is an instance of X2.
   * @throws RuntimeException if cause is not an instance of X1 or X2. The cause is wrapped with
   * {@link RuntimeException} if it is not already a {@link RuntimeException}.
   */
  public static <V, X1 extends Throwable, X2 extends Throwable> V execute(Transactional transactional,
                                                                          TxCallable<V> callable,
                                                                          Class<X1> exception1,
                                                                          Class<X2> exception2) throws X1, X2 {
    AtomicReference<V> result = new AtomicReference<>();
    try {
      transactional.execute(context -> result.set(callable.call(context)));
    } catch (TransactionFailureException e) {
      throw propagate(e, exception1, exception2);
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
  private static RuntimeException propagate(Throwable throwable) {
    propagateIfPossible(requireNonNull(throwable));
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
  public static <X extends Throwable> X propagate(TransactionFailureException e, Class<X> propagateType) throws X {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType);
    throw propagate(cause);
  }

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated types if the type matches or as {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @param propagateType1 if the exception is an instance of this type, it will be rethrown as is
   * @param propagateType2 if the exception is an instance of this type, it will be rethrown as is
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @return a exception of type X1,X2.
   */
  public static <X1 extends Throwable, X2 extends Throwable> X1 propagate(TransactionFailureException e,
                                                                          Class<X1> propagateType1,
                                                                          Class<X2> propagateType2) throws X1, X2 {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType1, propagateType2);
    throw propagate(cause);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@link RuntimeException} or {@link Error}.
   */
  private static void propagateIfPossible(@Nullable Throwable throwable) {
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
  private static <X extends Throwable> void propagateIfPossible(
    @Nullable Throwable throwable, Class<X> declaredType) throws X {
    propagateIfInstanceOf(throwable, declaredType);
    propagateIfPossible(throwable);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@link RuntimeException}, {@link Error}, {@code declaredType1},
   * or {@code declaredType2}.
   * @param throwable the Throwable to possibly propagate
   * @param declaredType1 any checked exception type declared by the calling
   *     method
   * @param declaredType2 any other checked exception type declared by the
   *     calling method
   */
  private static <X1 extends Throwable, X2 extends Throwable>
  void propagateIfPossible(@Nullable Throwable throwable,
                           Class<X1> declaredType1, Class<X2> declaredType2) throws X1, X2 {
    requireNonNull(declaredType2);
    propagateIfInstanceOf(throwable, declaredType1);
    propagateIfPossible(throwable, declaredType2);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an
   * instance of {@code declaredType}.
   */
  private static <X extends Throwable> void propagateIfInstanceOf(
    @Nullable Throwable throwable, Class<X> declaredType) throws X {
    if (throwable != null && declaredType.isInstance(throwable)) {
      throw declaredType.cast(throwable);
    }
  }

  /**
   * Returns the first of two given parameters that is not {@code null}, if
   * either is, or otherwise throws a {@link NullPointerException}.
   */
  private static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first != null ? first : requireNonNull(second);
  }
}
