package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import javax.annotation.Nullable;

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
  public <T> T getDataSetClient(String name, Class<? extends T> type, @Nullable Properties props) {
    try {
      return dataSetAccessor.getDataSetClient(name, type, props, DataSetAccessor.Namespace.USER);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> DataSetManager getDataSetManager(Class<? extends T> type) {
    try {
      return dataSetAccessor.getDataSetManager(type, DataSetAccessor.Namespace.USER);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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
