package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.util.Map;

/**
 * Helper to create DataSets
 */
public final class DataSets {

  public static Map<String, Closeable> createDataSets(DataSetContext context,
                                                    Iterable<String> datasets) {
    ImmutableMap.Builder<String, Closeable> builder = ImmutableMap.builder();

    for (String dataset : datasets) {
      Closeable dataSet = context.getDataSet(dataset);
      if (dataSet != null) {
        builder.put(dataset, dataSet);
      }
    }
    return builder.build();
  }

  private DataSets() {}
}
