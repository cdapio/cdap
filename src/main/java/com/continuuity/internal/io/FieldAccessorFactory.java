package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 *
 */
public interface FieldAccessorFactory {

  FieldAccessor getFieldAccessor(TypeToken<?> type, String fieldName);
}
