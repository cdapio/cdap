package com.continuuity.data.dataset;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;

public interface ApplicationContextBuilder {

  void setBatchCollectionClient(BatchCollectionClient client);
  void setDataFabric(DataFabric fabric);
  void addDataSet(DataSetMeta meta);

}
