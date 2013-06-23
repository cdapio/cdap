package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 * Factory for creating {@link DatumWriter}.
 */
public interface DatumWriterFactory {

  <T> DatumWriter<T> create(TypeToken<T> type, Schema schema);
}
