package com.continuuity.data2.transaction;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;

/**
 * Utility that wraps the execution of a function into the context of a transaction.
 */
// todo: implementations should throw different from TransactionFailureException in case of user code error?
// todo: accept only Callable? Executors util has a way to convert everything to Callable...
// TODO: Unify with TransactionalDatasetRegistry, see https://jira.continuuity.com/browse/REACTOR-324
public interface TransactionExecutor {

  /**
   * A function is a class with a single method that takes an argument and returns a result.
   * @param <I> the type of the argument
   * @param <O> the type of the result
   */
  public interface Function<I, O> {
    O apply(I input) throws Exception;
  }

  /**
   * A procedure is a class with a single void method that takes an argument.
   * @param <I> the type of the argument
   */
  public interface Procedure<I> {
    void apply(I input) throws Exception;
  }

  /**
   * A subroutine is a class with a single void method without arguments.
   */
  public interface Subroutine {
    void apply() throws Exception;
  }

  // Due to a bug in checkstyle, it would emit false positives here of the form
  // "Unable to get class information for @throws tag '<exn>' (...)".
  // This comment disables that check up to the corresponding ON comments below

  // CHECKSTYLE OFF: @throws

  /**
   * Execute a function under transactional semantics. A transaction is started  and all datasets
   * are initialized with the transaction. Then the passed function is executed, the transaction
   * is committed, and the function return value is returned as the return value of this method.
   * If any exception is caught, the transaction is aborted and the original exception is rethrown,
   * wrapped into a TransactionFailureException. If the transaction fails due to a write conflict,
   * a TransactionConflictException is thrown.
   * @param function the function to execute
   * @param input the input parameter for the function
   * @param <I> the input type of the function
   * @param <O> the result type of the function
   * @return the function's return value
   * @throws TransactionConflictException if there is a write conflict with another transaction.
   * @throws TransactionFailureException if any exception is caught, be it from the function or from the datasets.
   */
  <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException, InterruptedException;

  // CHECKSTYLE ON

  /**
   * Like {@link #execute(Function, Object)} but without a return value.
   */
  <I> void execute(Procedure<I> procedure, I input) throws TransactionFailureException, InterruptedException;

  /**
   * Like {@link #execute(Function, Object)} but the callable has no argument.
   */
  <O> O execute(Callable<O> callable) throws TransactionFailureException, InterruptedException;

  /**
   * Like {@link #execute(Function, Object)} but without argument or return value.
   */
  void execute(Subroutine subroutine) throws TransactionFailureException, InterruptedException;

  /**
   * Same as {@link #execute(Function, Object)} but
   * suppresses exception with {@link com.google.common.base.Throwables#propagate(Throwable)}
   */
  <I, O> O executeUnchecked(Function<I, O> function, I input);

  /**
   * Same as {@link #execute(Procedure, Object)} but
   * suppresses exception with {@link com.google.common.base.Throwables#propagate(Throwable)}
   */
  <I> void executeUnchecked(Procedure<I> procedure, I input);

  /**
   * Same as {@link #execute(Callable)} but
   * suppresses exception with {@link com.google.common.base.Throwables#propagate(Throwable)}
   */
  <O> O executeUnchecked(Callable<O> callable);

  /**
   * Same as {@link #execute(Subroutine)} but
   * suppresses exception with {@link com.google.common.base.Throwables#propagate(Throwable)}
   */
  void executeUnchecked(Subroutine subroutine);

  /**
   * Same as {@link #execute(Function, Object)} but executes asynchronously
   */
  <I, O> ListenableFuture<O> submit(Function<I, O> function, I input);

  /**
   * Same as {@link #execute(Procedure, Object)} but executes asynchronously
   */
  <I> ListenableFuture<?> submit(Procedure<I> procedure, I input);

  /**
   * Same as {@link #execute(Callable)} but executes asynchronously
   */
  <O> ListenableFuture<O> submit(Callable<O> callable);

  /**
   * Same as {@link #execute(Subroutine)} but executes asynchronously
   */
  ListenableFuture<?> submit(Subroutine subroutine);

}

