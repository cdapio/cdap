package com.continuuity.data2.transaction;

import java.util.concurrent.Callable;

/**
 * Utility that wraps the execution of a function into the context of a transaction.
 */
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
  <I, O> O execute(Function<I, O> function, I input) throws TransactionFailureException;

  /**
   * Like {@link #execute(Function, Object)} but without a return value.
   */
  <I> void execute(Procedure<I> procedure, I input) throws TransactionFailureException;

  /**
   * Like {@link #execute(Function, Object)} but the callable has no argument.
   */
  <O> O execute(Callable<O> callable) throws TransactionFailureException;

  /**
   * Like {@link #execute(Function, Object)} but without argument or return value.
   */
  void execute(Subroutine subroutine) throws TransactionFailureException;

}

