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
package co.cask.cdap.etl.common;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import org.apache.tephra.TransactionFailureException;

/**
 * Utility class to unwrap TransactionFailureException for proper handling.
 * This is similar to co.cask.cdap.data2.transaction.Transactions, but added here to avoid dependency
 * on cdap-data-fabric package
 */
public final class TransactionUtil {

  private TransactionUtil() {
  }

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is if it is a {@link RuntimeException}. Otherwise, the exception will be wrapped
   * with inside a {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @return a {@link RuntimeException}
   */
  public static RuntimeException propagate(TransactionFailureException e) {
    throw Throwables.propagate(Objects.firstNonNull(e.getCause(), e));
  }

  /**
   * Propagates the given {@link TransactionFailureException}. If the {@link TransactionFailureException#getCause()}
   * doesn't return {@code null}, the cause will be used instead for the propagation. This method will
   * throw the failure exception as-is the given propagated type if the type matches or as {@link RuntimeException}.
   * This method will always throw and the returned exception is for satisfying Java static analysis only.
   *
   * @param e the {@link TransactionFailureException} to propagate
   * @param propagateType if the exception is an instance of this type, it will be rethrown as is
   * @param <X> exception type of propagate type
   * @return a exception of type X.
   */
  public static <X extends Throwable> X propagate(TransactionFailureException e,
                                                  Class<X> propagateType) throws X {
    Throwable cause = Objects.firstNonNull(e.getCause(), e);
    Throwables.propagateIfPossible(cause, propagateType);
    throw Throwables.propagate(cause);
  }
}


