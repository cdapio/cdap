/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.CannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.NamespaceMeta;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface NamespaceClient {

  /**
   * Lists all namespaces.
   *
   * @return a list of {@link NamespaceMeta} for each namespace in CDAP.
   */
  public List<NamespaceMeta> list() throws IOException, UnAuthorizedAccessTokenException;

  /**
   * Retrieves details about a given namespace.
   *
   * @param namespaceId id of the namespace for which details are requested.
   * @return NamespaceMeta of the given namespace.
   */
  public NamespaceMeta get(String namespaceId) throws NotFoundException, IOException, UnAuthorizedAccessTokenException;

  /**
   * * Deletes a namespace from CDAP.
   *
   * @param namespaceId id of the namespace to be deleted.
   */
  public void delete(String namespaceId) throws NotFoundException, CannotBeDeletedException, IOException,
    UnAuthorizedAccessTokenException;

  /**
   * Creates a new namespace in CDAP
   *
   * @param namespaceMeta the {@link NamespaceMeta} for the namespace to be created
   */
  public void create(NamespaceMeta namespaceMeta) throws IOException, AlreadyExistsException, BadRequestException,
    UnAuthorizedAccessTokenException;
}
