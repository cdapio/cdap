/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.io;

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
