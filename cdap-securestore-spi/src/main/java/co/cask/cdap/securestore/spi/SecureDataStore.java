/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore.spi;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 *
 * TODO CDAP-14693 Add SPI to support delegation token refresh
 * TODO CDAP-14699 Expose dataset context to store metadata
 */
public interface SecureDataStore {

  String getType();

  /**
   *
   * @param context
   * @throws Exception
   */
  void initialize(SecureDataStoreContext context) throws Exception;

  /**
   *
   * @param namespace
   * @param name
   * @param data
   * @param description
   * @param properties
   * @throws Exception
   */
  void storeSecureData(String namespace, String name, byte[] data, String description,
                       Map<String, String> properties) throws Exception;

  /**
   *
   * @param namespace
   * @param name
   * @return
   * @throws Exception
   */
  Optional<SecureData> getSecureData(String namespace, String name) throws Exception;

  /**
   *
   * @param namespace
   * @return
   * @throws Exception
   */
  Collection<SecureData> getSecureData(String namespace) throws Exception;

  /**
   *
   * @param namespace
   * @param name
   * @throws Exception
   */
  void deleteSecureData(String namespace, String name) throws Exception;
}
