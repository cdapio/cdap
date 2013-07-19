/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 * Factory for creating instance of {@link DatumReader}.
 */
public interface DatumReaderFactory {

  /**
   * Creates a {@link DatumReader} that can decode object of type {@code T}.
   * @param type The object type to decode.
   * @param schema Schema of the object to decode to.
   * @param <T> Type of the object.
   * @return A {@link DatumReader}.
   */
  <T> DatumReader<T> create(TypeToken<T> type, Schema schema);
}
