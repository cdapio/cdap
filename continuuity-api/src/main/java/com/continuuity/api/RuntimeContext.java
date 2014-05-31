package com.continuuity.api;

import com.continuuity.api.data.DataSet;

import java.util.Map;

/**
 * This interface represents a context for a processor or elements of a processor.
 */
public interface RuntimeContext {
  /**
   * Given a name of dataset, returns an instance of {@link com.continuuity.api.data.DataSet}.
   * @param name of the {@link com.continuuity.api.data.DataSet}.
   * @param <T> The specific {@link com.continuuity.api.data.DataSet} type requested.
   * @return An instance of {@link com.continuuity.api.data.DataSet}.
   */
  <T extends DataSet> T getDataSet(String name);

  /**
   * @return A map of argument key and value.
   */
  Map<String, String> getRuntimeArguments();
}
