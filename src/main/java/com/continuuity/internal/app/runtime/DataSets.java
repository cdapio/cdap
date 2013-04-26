package com.continuuity.internal.app.runtime;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.dataset.Stream;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Helper to create DataSets
 */
public final class DataSets {

  public static Map<String, DataSet> createDataSets(DataSetContext context, OVCTableHandle handle,
                                                    Iterable<String> datasets) throws OperationException {
    ImmutableMap.Builder<String, DataSet> builder = ImmutableMap.builder();

    for (String dataset : datasets) {
      if (context.hasDataSet(dataset)){
        builder.put(dataset, context.getDataSet(dataset));
      } else {
        Stream streamDataSet = new Stream(dataset,handle);
        builder.put(dataset,streamDataSet);
      }
    }
    return builder.build();
  }

  private DataSets() {}


}
