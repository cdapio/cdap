/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.api.security.credential;

/**
 * An entity which holds secure data and metadata. The secured data is stored in SecureStore,
 * whereas the metadata is stored elsewhere.
 *
 * @param <T> The metadata class.
 */
public interface Secured<T> {

  /**
   * @return The metadata for the entity.
   */
  T getMetadata();

  /**
   * @return The secure data stored by the entity.
   */
  String getSecureData();
}
