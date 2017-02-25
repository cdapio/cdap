/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Provides implementation of asynchronous methods of {@link TransactionExecutor} by delegating their execution
 * to respective synchronous methods via provided {@link ExecutorService}.
 */
public abstract class AbstractTransactionExecutor implements TransactionExecutor {
  private final ListeningExecutorService executorService;

  protected AbstractTransactionExecutor(ExecutorService executorService) {
    this.executorService = MoreExecutors.listeningDecorator(executorService);
  }

  @Override
  public <I, O> O executeUnchecked(Function<I, O> function, I input) {
    try {
      return execute(function, input);
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <I> void executeUnchecked(Procedure<I> procedure, I input) {
    try {
      execute(procedure, input);
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <O> O executeUnchecked(Callable<O> callable) {
    try {
      return execute(callable);
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void executeUnchecked(Subroutine subroutine) {
    try {
      execute(subroutine);
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <I, O> ListenableFuture<O> submit(final Function<I, O> function, final I input) {
    return executorService.submit(new Callable<O>() {
      @Override
      public O call() throws Exception {
        return execute(function, input);
      }
    });
  }

  @Override
  public <I> ListenableFuture<?> submit(final Procedure<I> procedure, final I input) {
    return executorService.submit(new Callable<Object>() {
      @Override
      public I call() throws Exception {
        execute(procedure, input);
        return null;
      }
    });
  }

  @Override
  public <O> ListenableFuture<O> submit(final Callable<O> callable) {
    return executorService.submit(new Callable<O>() {
      @Override
      public O call() throws Exception {
        return execute(callable);
      }
    });
  }

  @Override
  public ListenableFuture<?> submit(final Subroutine subroutine) {
    return executorService.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        execute(subroutine);
        return null;
      }
    });
  }
}
