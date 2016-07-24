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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.List;

/**
 * Service interface for the secure store.
 */
public interface SecureStoreService {

  /**
   * Lists all the keys that belong to the given namespace.
   * @param namespaceId Id of the namespace we want the key list for.
   * @return A list of {@link SecureKeyListEntry} of all the keys visible to the user under the given namespace.
   */
  List<SecureKeyListEntry> list(NamespaceId namespaceId) throws Exception;

  /**
   * Returns the data associated with the key.
   * @param secureKeyId Id of the key that the user is trying to read.
   * @return Data associated with the key.
   */
  SecureStoreData get(SecureKeyId secureKeyId) throws Exception;

  /**
   * Puts the user provided data in the secure store.
   * @param secureKeyId The Id for the key that needs to be stored.
   * @param secureKeyCreateRequest The request containing the data to be stored in the secure store.
   */
  void put(SecureKeyId secureKeyId, SecureKeyCreateRequest secureKeyCreateRequest)
    throws Exception;

  /**
   * Deletes the key.
   * @param secureKeyId Id of the key to be deleted
   */
  void delete(SecureKeyId secureKeyId) throws Exception;
}
