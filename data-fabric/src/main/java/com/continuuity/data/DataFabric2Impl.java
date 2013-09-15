package com.continuuity.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
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
 * TODO: will replace DataFabricImpl when integration with txds2 refactoring is done or will go away completely
 */
public class DataFabric2Impl implements DataFabric {

  private LocationFactory locationFactory;
  private DataSetAccessor dataSetAccessor;

  public DataFabric2Impl(LocationFactory locationFactory,
                         DataSetAccessor dataSetAccessor) {
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
    // no-op
    return null;
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> read(ReadColumnRange readColumnRange) throws OperationException {
    // no-op
    return null;
  }

  @Override
  public void execute(Write write) throws OperationException {
    // no-op
  }

  @Override
  public void execute(Delete delete) throws OperationException {
    // no-op
  }

  @Override
  public void execute(Increment inc) throws OperationException {
    // no-op
  }

  @Override
  public void execute(CompareAndSwap cas) throws OperationException {
    // no-op
  }

  @Override
  public void execute(List<WriteOperation> writes) throws OperationException {
    // no-op
  }

  @Override
  public void openTable(String name) throws OperationException {
    // no-op
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
