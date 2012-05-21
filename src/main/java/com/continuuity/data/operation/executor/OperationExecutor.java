package com.continuuity.data.operation.executor;

import com.continuuity.data.operation.type.ReadOperation;

/**
 * Executes read and write operations.
 *
 * Reads return an object as specified by the type of {@link ReadOperation}.
 *
 * Writes return true or false as to whether they succeeded or not.
 */
public interface OperationExecutor
extends ReadOperationExecutor, WriteOperationExecutor, BatchOperationExecutor {

}
