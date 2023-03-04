/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * This Decoder wraps another decoder and uses cache to establish identity mapping of string values.
 * It's extremely valuable to reduce memory footprint of deserialized values with same strings.
 * Pretty often when values are serialized, it's string fields refer to constants and don't consume
 * much memory. But with JSON or binary deserialization each such constant is deserialized
 * separately increasing memory footprint multifold. This decoder aims to solve this problem.
 *
 * There are two approaches to use it: you can pass a real concurrent LRU cache or, if you have a
 * set of values to deserialize in a single thread, simply pass a {@link HashMap} and throw it away
 * after all values are deserialized. The HashMap is much faster than a real concurrent hash, but
 * can be used only in cetain scenarios.
 */
public class StringCachingDecoder implements Decoder {

  private final Decoder delegate;
  private final Map<String, String> cache;

  /**
   * @param delegate Decoder to delegate to
   * @param cache map to be used as a cache.
   */
  public StringCachingDecoder(Decoder delegate, Map<String, String> cache) {
    this.delegate = delegate;
    this.cache = cache;
  }

  /**
   * @return map used as a cache
   */
  public Map<String, String> getCache() {
    return cache;
  }

  @Override
  @Nullable
  public Object readNull() throws IOException {
    return delegate.readNull();
  }

  @Override
  public boolean readBool() throws IOException {
    return delegate.readBool();
  }

  @Override
  public int readInt() throws IOException {
    return delegate.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return delegate.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return delegate.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return delegate.readDouble();
  }

  @Override
  public String readString() throws IOException {
    return cache.computeIfAbsent(delegate.readString(), Function.identity());
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    return delegate.readBytes();
  }

  @Override
  public void skipFloat() throws IOException {
    delegate.skipFloat();
  }

  @Override
  public void skipDouble() throws IOException {
    delegate.skipDouble();
  }

  @Override
  public void skipString() throws IOException {
    delegate.skipString();
  }

  @Override
  public void skipBytes() throws IOException {
    delegate.skipBytes();
  }
}
