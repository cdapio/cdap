package com.continuuity.data.dataset;

import com.continuuity.api.data.DataFabric;

import java.util.HashMap;
import java.util.Map;

public class ApplicationContextImpl {

  private DataFabric fabric;
  private SimpleBatchCollectionClient collectionClient;

  Map<String, DataSetMeta> datasets = new HashMap<String, DataSetMeta>();

  public void addDataSet(DataSetMeta meta) {
    datasets.put(meta.getName(), meta);
  }

  public
  <T extends DataSet> T getDataSet(String name) {
    try {
      DataSetMeta meta = this.datasets.get(name);
      Class<?> dsClass = Class.forName(meta.getType());
      T ds = (T) dsClass.getConstructor(DataSetMeta.class).newInstance(meta);
      return ds;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

}

