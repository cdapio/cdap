/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.builder;

import java.util.Map;

/**
 * @param <T>
 */
public interface OptionsSetter<T> {
  T withOptions(Map<String, String> options);
}
