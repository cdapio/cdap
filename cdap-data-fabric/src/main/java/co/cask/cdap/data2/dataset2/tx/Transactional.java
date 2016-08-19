/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.tx;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handy utility for performing transactional operations that delegates execution to {@link TransactionExecutor}
 * and manages resources in transaction context. Transaction context is supplied using given {@link Supplier} and
 * provides list of resources by implementing {@link Iterable}. This applies transaction logic to those resources
 * that implement {@link TransactionAware}. Additionally, if resource implements {@link Closeable} its
 * {@link java.io.Closeable#close()} is invoked at the end of transaction.
 *
 * @param <T> type of the transactional context
 * @param <V> type of objects contained inside the transaction context
 *
 * @deprecated Don't use this class anymore. Use {@link TransactionExecutor} instead.
 */
@Deprecated
public class Transactional<T extends Iterable<V>, V> {
  private final TransactionExecutorFactory txFactory;
  private final Supplier<T> supplier;

  public static <T extends Iterable<V>, V> Transactional<T, V> of(TransactionExecutorFactory txFactory,
                                                                  Supplier<T> supplier) {
    return new Transactional<>(txFactory, supplier);
  }

  /**
   * Creates instance of {@link Transactional}.
   * @param txFactory factory for {@link TransactionExecutor}s
   * @param supplier supplies transaction context. Transaction logic will be applied to the items returned by the
   *                 context's getIterator() method for those that implement {@link TransactionAware}
   */
  private Transactional(TransactionExecutorFactory txFactory, Supplier<T> supplier) {
    this.txFactory = txFactory;
    this.supplier = supplier;
  }

  /**
   * Executes given function within new transaction.
   */
  public <R> R execute(final TransactionExecutor.Function<T, R> func)
    throws TransactionFailureException, InterruptedException, IOException {

    return execute(txFactory, supplier, func);
  }

  /**
   * Executes given function within new transaction and rethrows all exceptions with
   * {@link Throwables#propagate(Throwable)}
   */
  public <R> R executeUnchecked(final TransactionExecutor.Function<T, R> func) {
    try {
      return execute(txFactory, supplier, func);
    } catch (IOException | TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  /**
   * Executes function within new transaction. See {@link Transactional} for more details.
   * @param txFactory transaction factory to create new transaction
   * @param supplier supplier of transaction context
   * @param func function to execute
   * @param <V> type of object contained inside the transaction context
   * @param <T> type of the transaction context
   * @param <R> type of the function result
   * @return function result
   */
  public static <V, T extends Iterable<V>, R> R execute(TransactionExecutorFactory txFactory,
                                                        Supplier<T> supplier,
                                                        TransactionExecutor.Function<T, R> func)
    throws TransactionFailureException, IOException, InterruptedException {

    T it = supplier.get();
    Iterable<TransactionAware> txAwares = Iterables.transform(
      Iterables.filter(it, Predicates.instanceOf(TransactionAware.class)), new Function<V, TransactionAware>() {
      @Override
      public TransactionAware apply(V input) {
        return (TransactionAware) input;
      }
    });

    TransactionExecutor executor = txFactory.createExecutor(txAwares);
    try {
      return executor.execute(func, it);
    } finally {
      for (V t : it) {
        if (t instanceof Closeable) {
          ((Closeable) t).close();
        }
      }
    }
  }
}
