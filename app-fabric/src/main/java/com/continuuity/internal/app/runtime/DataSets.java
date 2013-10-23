package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Helper to create DataSets
 */
public final class DataSets {

  public static Map<String, DataSet> createDataSets(DataSetContext context,
                                                    Iterable<String> datasets) {
    ImmutableMap.Builder<String, DataSet> builder = ImmutableMap.builder();

    for (String dataset : datasets) {
        builder.put(dataset, context.getDataSet(dataset));
    }
    return builder.build();
  }

  private DataSets() {}


}
