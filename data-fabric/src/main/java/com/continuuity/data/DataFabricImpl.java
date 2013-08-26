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
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of the DataFabric interface.
 */
public class DataFabricImpl implements DataFabric {

  private OperationExecutor opex;
  private OperationContext context;
  private LocationFactory locationFactory;
  private DataSetAccessor dataSetAccessor;

  public DataFabricImpl(OperationExecutor opex,
                        LocationFactory locationFactory,
                        DataSetAccessor dataSetAccessor,
                        OperationContext context) {
    this.opex = opex;
    this.context = context;
    this.locationFactory = locationFactory;
    this.dataSetAccessor = dataSetAccessor;
  }

  // These are to support new TxDs2 system. DataFabric will go away once we fully migrate to it.
  @Override
  public <T> T getDataSetClient(String name, Class<? extends T> type) {
    try {
      return dataSetAccessor.getDataSetClient(name, type);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> DataSetManager getDataSetManager(Class<? extends T> type) {
    try {
      return dataSetAccessor.getDataSetManager(type);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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

  @Override
  public Location getLocation(String path) throws IOException {
    return this.locationFactory.create(path);
  }

  @Override
  public Location getLocation(URI uri) throws IOException {
    return this.locationFactory.create(uri);
  }
}
