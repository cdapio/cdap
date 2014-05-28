package com.continuuity.common.io;

import java.io.IOException;

/**
 * Common interface representing a utility which knows how to serialize and deserialize a given type of object.
 * @param <T> the class type which can be serialized / deserialized.
 */
public interface Codec<T> {
  /**
   * Returns the serialized form of the given object as a {@code byte[]}.
   */
  byte[] encode(T object) throws IOException;

  /**
   * Returns the deserialized object represented in the given {@code byte[]}.
   */
  T decode(byte[] data) throws IOException;
}
