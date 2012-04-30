package com.continuuity.persistence.ops.execs;

import java.util.List;

import com.continuuity.fabric.operations.Operation;

/**
 * The role of an OperationsExecutor is to convert a set of {@link Operations}
 * into a set of {@link NativeOperations}.
 */
public interface OperationsExecutor {

	public void execute(List<Operation> operations)
	    throws OperationExecutionException;
}
