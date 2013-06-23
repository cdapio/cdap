package com.continuuity.internal.io;

import com.continuuity.common.io.Encoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public interface DatumWriter<T> {

  void encode(T data, Encoder encoder) throws IOException;
}
