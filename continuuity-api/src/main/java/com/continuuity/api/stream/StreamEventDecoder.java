/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents decoder that turns {@link StreamEvent} into key value pair.
 *
 * @param <K> Type of key that this class decodes.
 * @param <V> Type of value that this class decodes.
 */
public interface StreamEventDecoder<K, V>  {

  /**
   * Decode a {@link StreamEvent}.
   *
   * @param event The event to be decoded.
   * @param result Reusable object for putting decode result.
   * @return The decode result. It can be the same instance as the one provided as argument.
   */
  DecodeResult<K, V> decode(StreamEvent event, DecodeResult<K, V> result);

  /**
   * Represents the decoded result pair.
   *
   * @param <K> Type of key.
   * @param <V> Type of value.
   */
  @NotThreadSafe
  static final class DecodeResult<K, V> {
    private K key;
    private V value;

    public K getKey() {
      return key;
    }

    public DecodeResult<K, V> setKey(K key) {
      this.key = key;
      return this;
    }

    public V getValue() {
      return value;
    }

    public DecodeResult<K, V> setValue(V value) {
      this.value = value;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DecodeResult decodeResult = (DecodeResult) o;
      return key.equals(decodeResult.key) && value.equals(decodeResult.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, value);
    }
  }
}
