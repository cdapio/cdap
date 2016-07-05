/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.security.store;

/**
 * This represents the entity stored in the Secure Store.
 * The data is stored as UTF8 encoded byte array.
 * An instance of an implementation of SecureStoreData is created and returned from the
 * {@link SecureStore}'s get method call.
 */
public final class SecureStoreData {

  private final SecureStoreMetadata metadata;
  private final byte[] data;

  public SecureStoreData(SecureStoreMetadata metadata, byte[] data) {
    this.metadata = metadata;
    this.data = data;
  }

  /**
   * @return Returns an object representing the metadata associated with this element.
   * The metadata for an element contains its name, description, creation time and a map of all the
   * element's properties set by the user.
   */
  public SecureStoreMetadata getMetadata() {
    return metadata;
  }

  /**
   * @return The data stored in a utf-8 encoded byte array
   */
  public byte[] get() {
    return data;
  }
}
