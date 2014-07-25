/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset2.tx;

import com.continuuity.tephra.TransactionAware;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionExecutorFactory;
import com.continuuity.tephra.TransactionFailureException;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utility to perform transactional operations
 * @param <T> type of the transactional context
 */
public class Transactional<T extends Iterable> {
  private final TransactionExecutorFactory txFactory;
  private final Supplier<T> supplier;

  /**
   * Ctor.
   * @param txFactory factory for {@link TransactionExecutor}s
   * @param supplier supplies transaction context. Transaction logic will be applied to the items returned by the
   *                 context's getIterator() method for those that implement {@link TransactionAware}
   */
  public Transactional(TransactionExecutorFactory txFactory, Supplier<T> supplier) {
    this.txFactory = txFactory;
    this.supplier = supplier;
  }

  public <R> R execute(final TransactionExecutor.Function<T, R> func)
    throws TransactionFailureException, InterruptedException, IOException {

    return execute(txFactory, supplier, func);
  }

  public <R> R executeUnchecked(final TransactionExecutor.Function<T, R> func) {
    try {
      return execute(txFactory, supplier, func);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  public static <T extends Iterable<?>, R> R execute(TransactionExecutorFactory txFactory,
                                 Supplier<T> supplier,
                                 final TransactionExecutor.Function<T, R> func)
    throws TransactionFailureException, IOException, InterruptedException {

    T it = supplier.get();
    Iterable<TransactionAware> txAwares = Iterables.transform(
      Iterables.filter(it, Predicates.instanceOf(TransactionAware.class)), new Function<Object, TransactionAware>() {
      @Override
      public TransactionAware apply(Object input) {
        return (TransactionAware) input;
      }
    });

    TransactionExecutor executor = txFactory.createExecutor(txAwares);
    try {
      return executor.execute(func, it);
    } finally {
      for (Object t : it) {
        if (t instanceof Closeable) {
          ((Closeable) t).close();
        }
      }
    }
  }
}
