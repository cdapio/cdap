/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.data.transaction;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for interacting with {@link TransactionRunner}.
 * TODO: Figure out better way to propagate the exception: https://issues.cask.co/browse/CDAP-14736
 */
public class TransactionRunners {

  // private constructor to prevent instantiation of this class.
  private TransactionRunners() {
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @throws RuntimeException where the cause is wrapped with {@link RuntimeException} if it is not already a
   * {@link RuntimeException}
   */
  public static void run(TransactionRunner txRunner, TxRunnable runnable) {
    try {
      txRunner.run(runnable);
    } catch (TransactionException e) {
      throw propagate(e);
    }
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @param <X> exception type of propagate type
   * @throws X if failed to execute the given {@link co.cask.cdap.api.TxRunnable} in a transaction.
   * If the TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X.
   * @throws RuntimeException if cause is not an instance of X. The cause is wrapped with {@link RuntimeException}
   * if it is not already a {@link RuntimeException}.
   */
  public static <X extends Throwable> void run(TransactionRunner txRunner,
                                               TxRunnable runnable, Class<X> exception) throws X {
    try {
      txRunner.run(runnable);
    } catch (TransactionException e) {
      throw propagate(e, exception);
    }
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @throws X1 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X1.
   * @throws X2 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X2.
   * @throws RuntimeException if cause is not an instance of X1 or X2. The cause is wrapped with
   * {@link RuntimeException} if it is not already a {@link RuntimeException}.
   */
  public static <X1 extends Throwable, X2 extends Throwable> void run(TransactionRunner txRunner,
                                                                      TxRunnable runnable,
                                                                      Class<X1> exception1,
                                                                      Class<X2> exception2) throws X1, X2 {
    try {
      txRunner.run(runnable);
    } catch (TransactionException e) {
      throw propagate(e, exception1, exception2);
    }
  }

  /**
   * Executes the given {@link TxRunnable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param runnable the {@link TxRunnable} to be executed inside a transaction
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @param <X3> exception type of third propagate type
   * @throws X1 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X1.
   * @throws X2 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X2.
   * @throws X3 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X3.
   * @throws RuntimeException if cause is not an instance of X1 or X2 or X3. The cause is wrapped with
   * {@link RuntimeException} if it is not already a {@link RuntimeException}.
   */
  public static <X1 extends Throwable, X2 extends Throwable, X3 extends Throwable>
  void run(TransactionRunner txRunner,
           TxRunnable runnable,
           Class<X1> exception1,
           Class<X2> exception2,
           Class<X3> exception3) throws X1, X2, X3 {
    try {
      txRunner.run(runnable);
    } catch (TransactionException e) {
      throw propagate(e, exception1, exception2, exception3);
    }
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @return value returned by the given {@link TxCallable}
   * @throws  RuntimeException if failed to execute the given {@link TxRunnable} in a transaction.
   * If the TransactionException has a cause in it, the cause is propagated.
   */
  public static <V> V run(TransactionRunner txRunner, TxCallable<V> callable) {
    try {
      AtomicReference<V> result = new AtomicReference<>();
      txRunner.run(context -> result.set(callable.call(context)));
      return result.get();
    } catch (TransactionException e) {
      throw propagate(e);
    }
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for the transaction execution
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @param <X> exception type of propagate type
   * @return value returned by the given {@link TxCallable}
   * @throws X if failed to execute the given {@link TxRunnable} in a transaction. If the TransactionException
   * has a cause in it, the cause is thrown as-is if it is an instance of X.
   * @throws RuntimeException if cause is not an instance of X. The cause is wrapped with {@link RuntimeException}
   * if it is not already a {@link RuntimeException}.
   */
  public static <V, X extends Throwable> V run(TransactionRunner txRunner,
                                               TxCallable<V> callable, Class<X> exception) throws X {
    try {
      AtomicReference<V> result = new AtomicReference<>();
      txRunner.run(c -> result.set(callable.call(c)));
      return result.get();
    } catch (TransactionException e) {
      throw propagate(e, exception);
    }
  }

  /**
   * Executes the given {@link TxCallable} using the given {@link TransactionRunner}.
   *
   * @param txRunner the {@link TransactionRunner} to use for txRunner execution
   * @param callable the {@link TxCallable} to be executed inside a transaction
   * @param <V> type of the result
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @return value returned by the given {@link TxCallable}
   * @throws X1 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X1.
   * @throws X2 if failed to execute the given {@link TxRunnable} in a transaction. If the
   * TransactionException has a cause in it, the cause is thrown as-is if it is an instance of X2.
   * @throws RuntimeException if cause is not an instance of X1 or X2. The cause is wrapped with
   * {@link RuntimeException} if it is not already a {@link RuntimeException}.
   */
  public static <V, X1 extends Throwable, X2 extends Throwable> V run(TransactionRunner txRunner,
                                                                      TxCallable<V> callable,
                                                                      Class<X1> exception1,
                                                                      Class<X2> exception2) throws X1, X2 {
    try {
      AtomicReference<V> result = new AtomicReference<>();
      txRunner.run(context -> result.set(callable.call(context)));
      return result.get();
    } catch (TransactionException e) {
      throw propagate(e, exception1, exception2);
    }
  }

  /**
   * Propagates {@code throwable} as-is if it is an instance of
   * {@link RuntimeException} or {@link Error}, or else as a last resort, wraps
   * it in a {@code RuntimeException} then propagates.
   *
   * @param throwable the Throwable to propagate
   * @return nothing will ever be returned. This return type is only for convenience.
   */
  private static RuntimeException propagate(Throwable throwable) {
    propagateIfPossible(requireNonNull(throwable));
    throw new RuntimeException(throwable);
  }

  /**
   * Propagates the given {@link TransactionException}. If the {@link TransactionException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated type if the type matches or as {@link RuntimeException}.
   *
   * @param e the {@link TransactionException} to propagate
   * @param propagateType if the exception is an instance of this type, it will be rethrown as is
   * @param <X> exception type of propagate type
   */
  public static <X extends Throwable> X propagate(TransactionException e, Class<X> propagateType) throws X {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType);
    throw propagate(cause);
  }

  /**
   * Propagates the given {@link TransactionException}. If the {@link TransactionException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated types if the type matches or as {@link RuntimeException}.
   *
   * @param e the {@link TransactionException} to propagate
   * @param propagateType1 if the exception is an instance of this type, it will be rethrown as is
   * @param propagateType2 if the exception is an instance of this type, it will be rethrown as is
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   */
  public static <X1 extends Throwable, X2 extends Throwable> X1 propagate(TransactionException e,
                                                                          Class<X1> propagateType1,
                                                                          Class<X2> propagateType2) throws X1, X2 {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType1, propagateType2);
    throw propagate(cause);
  }

  /**
   * Propagates the given {@link TransactionException}. If the {@link TransactionException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated types if the type matches or as {@link RuntimeException}.
   *
   * @param e the {@link TransactionException} to propagate
   * @param propagateType1 if the exception is an instance of this type, it will be rethrown as is
   * @param propagateType2 if the exception is an instance of this type, it will be rethrown as is
   * @param propagateType3 if the exception is an instance of this type, it will be rethrown as is
   * @param <X1> exception type of first propagate type
   * @param <X2> exception type of second propagate type
   * @param <X3> exception type of third propagate type
   */
  public static <X1 extends Throwable, X2 extends Throwable, X3 extends Throwable> X1
  propagate(TransactionException e,
            Class<X1> propagateType1,
            Class<X2> propagateType2,
            Class<X3> propagateType3) throws X1, X2, X3 {
    Throwable cause = firstNonNull(e.getCause(), e);
    propagateIfPossible(cause, propagateType1, propagateType2);
    propagateIfPossible(cause, propagateType3);
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
   *
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
   *
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
    if (declaredType.isInstance(throwable)) {
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
