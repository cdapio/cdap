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

package io.cdap.cdap.security.store.file;

import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.common.io.Codec;
import java.security.KeyStore;

/**
 * Codec interface for {@link SecureStoreData} and reading the {@link KeyStore}.
 *
 * This Codec defines support for reading from KeyStore as well as reading and writing keys.
 */
public interface FileSecureStoreCodec extends Codec<SecureStoreData> {

  /**
   * Returns the supported KeyStore scheme for the codec.
   *
   * @return The KeyStore scheme.
   */
  String getKeyStoreScheme();

  /**
   * Defines how to construct the key alias from a KeyInfo.
   *
   * @param keyInfo The key information for constructing a KeyStore alias
   * @return The alias of the key
   */
  String getKeyAliasFromInfo(KeyInfo keyInfo);

  /**
   * Defins how to construct the KeyInfo from a key alias.
   *
   * @param keyAlias The alias of the key
   * @return The key information
   */
  KeyInfo getKeyInfoFromAlias(String keyAlias);

  /**
   * Defines how to retrieve the prefix used for namespace searching.
   *
   * @param namespace The namespace to search
   * @return The alias prefix
   */
  String getAliasSearchPrefix(String namespace);
}
