package com.continuuity.api;

import com.continuuity.api.data.DataSetContext;

import java.util.Map;

/**
 * This interface represents a context for a processor or elements of a processor.
 */
public interface RuntimeContext extends DataSetContext {
  /**
   * @return A map of argument key and value.
   */
  Map<String, String> getRuntimeArguments();
}
