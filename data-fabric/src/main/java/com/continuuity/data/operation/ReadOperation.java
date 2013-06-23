package com.continuuity.data.operation;

/**
 * An {@link Operation} that reads and returns data.
 */
public abstract class ReadOperation extends Operation {

  protected ReadOperation() {
    // do nothing
  }

  protected ReadOperation(long id) {
    super(id);
  }
}


