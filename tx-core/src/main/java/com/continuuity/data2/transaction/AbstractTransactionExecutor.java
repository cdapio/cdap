package com.continuuity.data2.transaction;

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
