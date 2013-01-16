package com.continuuity.data;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.executor.OperationExecutor;

import java.util.List;
import java.util.Map;

public class DataFabricImpl implements DataFabric {

  private OperationExecutor opex;
  private OperationContext context;

  public DataFabricImpl(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public OperationResult<byte[]> read(ReadKey read) throws OperationException {
    return this.opex.execute(context, read);
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
    this.opex.execute(context, write);
  }

  @Override
  public void execute(Delete delete) throws OperationException {
    this.opex.execute(context, delete);
  }

  @Override
  public void execute(Increment inc) throws OperationException {
    this.opex.execute(context, inc);
  }

  @Override
  public void execute(CompareAndSwap cas) throws OperationException {
    this.opex.execute(context, cas);
  }

  @Override
  public void execute(List<WriteOperation> writes) throws OperationException {
    this.opex.execute(context, writes);
  }

  @Override
  public void openTable(String name) throws OperationException {
    this.opex.execute(context, new OpenTable(name));
  }


}
