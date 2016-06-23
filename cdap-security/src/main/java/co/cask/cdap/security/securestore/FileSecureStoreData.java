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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.securestore.SecureStoreData;
import co.cask.cdap.api.security.securestore.SecureStoreMetadata;

/**
 * Represents the data stored in the Secure Store.
 */
public class FileSecureStoreData implements SecureStoreData {

  private final SecureStoreMetadata metadata;
  private final byte[] data;

  FileSecureStoreData(SecureStoreMetadata metadata, byte[] data) {
    this.metadata = metadata;
    this.data = data;
  }
  /**
   * @return Returns an object representing the metadata associated with this data.
   */
  @Override
  public SecureStoreMetadata getMetadata() {
    return metadata;
  }

  /**
   * @return The data stored in a utf-8 encoded byte array
   */
  @Override
  public byte[] get() {
    return data;
  }
}
