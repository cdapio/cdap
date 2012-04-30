package com.continuuity.fabric.operations;

/**
 * Executes read and write operations.
 *
 * Reads return an object as specified by the type of {@link ReadOperation}.
 *
 * Writes return true or false as to whether they succeeded or not.
 */
public interface OperationExecutor
extends ReadOperationExecutor, WriteOperationExecutor {

}
