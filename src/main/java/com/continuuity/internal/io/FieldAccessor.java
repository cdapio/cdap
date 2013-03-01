package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 *
 */
public interface FieldAccessor {

  void set(Object object, Object value);

  Object get(Object object);

  TypeToken<?> getType();
}
