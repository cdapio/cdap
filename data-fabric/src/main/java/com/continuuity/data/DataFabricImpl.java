package com.continuuity.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;

import java.util.List;
import java.util.Map;

/**
 * Simple implementation of the DataFabric interface.
 */
public class DataFabricImpl implements DataFabric {

  private OperationExecutor opex;
  private OperationContext context;

  public DataFabricImpl(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read) throws OperationException {
    return this.opex.execute(context, read);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> read(ReadColumnRange readColumnRange) throws OperationException {
    return this.opex.execute(context, readColumnRange);
  }

  @Override
  public void execute(Write write) throws OperationException {
    this.opex.commit(context, write);
  }

  @Override
  public void execute(Delete delete) throws OperationException {
    this.opex.commit(context, delete);
  }

  @Override
  public void execute(Increment inc) throws OperationException {
    this.opex.increment(context, inc);
  }

  @Override
  public void execute(CompareAndSwap cas) throws OperationException {
    this.opex.commit(context, cas);
  }

  @Override
  public void execute(List<WriteOperation> writes) throws OperationException {
    this.opex.commit(context, writes);
  }

  @Override
  public void openTable(String name) throws OperationException {
    this.opex.execute(context, new OpenTable(name));
  }


}
