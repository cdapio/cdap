/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.securestore.spi.secret;

import java.io.IOException;

/**
 * Encodes the object of provided type.
 * @param <T> type of the object to be encoded
 */
public interface Encoder<T> {
  /**
   * Encodes the provided data.
   *
   * @param data data to be encoded
   * @return encoded object in bytes
   * @throws IOException if there's any error while encoding data
   */
  byte[] encode(T data) throws IOException;
}
