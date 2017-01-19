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

package co.cask.cdap.common.security;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.ImplementedBy;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Responsible for executing code for a user, configurable at the namespace level.
 *
 * TODO: CDAP-1698. Ideally there should be an explicit binding in some Module. However, adding that is too complicate
 * now, due to the poor organization of Guice usage in CDAP.
 */
@ImplementedBy(DefaultImpersonator.class)
public interface Impersonator {
  /**
   * Executes a callable as the user, configurable at a namespace level
   *
   * @param namespaceId the namespace to use to lookup the user
   * @param callable the callable to execute
   * @param <T> return type of the callable
   *
   * @return the return value of the callable
   * @throws NamespaceNotFoundException if the namespace does not exist
   * @throws Exception if the callable throws any exception
   */
  <T> T doAs(NamespaceId namespaceId, Callable<T> callable) throws Exception;

  /**
   * Executes a callable as the user, configurable at a namespace level
   *
   * @param namespaceMeta the metadata of the namespace to use to lookup the user
   * @param callable the callable to execute
   * @param <T> return type of the callable
   *
   * @return the return value of the callable
   * @throws Exception if the callable throws any exception
   */
  <T> T doAs(NamespaceMeta namespaceMeta, Callable<T> callable) throws Exception;

  /**
   * Retrieve the {@link UserGroupInformation} for the given {@link NamespaceId}
   *
   * @param namespaceId namespace to lookup the user
   * @return {@link UserGroupInformation}
   * @throws IOException if there was any error fetching the {@link UserGroupInformation}
   * @throws NamespaceNotFoundException if namespaceId does not exist
   */
  UserGroupInformation getUGI(NamespaceId namespaceId) throws IOException, NamespaceNotFoundException;
}
