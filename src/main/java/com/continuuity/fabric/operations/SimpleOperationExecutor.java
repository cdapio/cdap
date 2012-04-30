package com.continuuity.fabric.operations;

public interface SimpleOperationExecutor extends OperationExecutor {

  /**
   * Performs the specified writes synchronously and in sequence.
   *
   * If an error is reached, execution of subsequent operations is skipped and
   * false is returned.  If all operations are performed successfully, returns
   * true.
   *
   * @param writes write operations to be performed in sequence
   * @return true if all operations succeeded, false if not
   */
  @Override
  public boolean execute(WriteOperation [] writes);
}
