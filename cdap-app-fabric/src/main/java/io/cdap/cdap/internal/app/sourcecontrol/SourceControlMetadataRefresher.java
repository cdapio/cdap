/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.internal.app.services.NamespaceSourceControlMetadataRefresher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.RepositoryTable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages a mapping of namespaces with repository configurations to their respective
 * NamespaceSourceControlMetadataRefreshService scheduler instances.
 */
public class SourceControlMetadataRefresher {
  private ConcurrentMap<NamespaceId, Long>
      lastRefreshTimes = new ConcurrentHashMap<>();
  private final TransactionRunner transactionRunner;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final SecureStore secureStore;
  private static final Logger LOG = LoggerFactory.getLogger(
      SourceControlMetadataRefresher.class);

  /**
   * Constructs a SourceControlMetadataRefreshService instance.
   */
  @Inject
  public SourceControlMetadataRefresher(TransactionRunner transactionRunner,
      SourceControlOperationRunner sourceControlOperationRunner, SecureStore secureStore) {
    this.transactionRunner = transactionRunner;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.secureStore = secureStore;
  }

  private NamespaceSourceControlMetadataRefresher createRefreshService(NamespaceId namespace) {
    return new NamespaceSourceControlMetadataRefresher(transactionRunner,
        sourceControlOperationRunner, namespace);
  }

  /**
   * Retrieves the timestamp of the last refresh for the specified namespace.
   *
   * @param namespaceId The ID of the namespace for which to retrieve the last refresh time.
   * @return The timestamp of the last refresh
   */
  public long getLastRefreshTime(NamespaceId namespaceId) {
    return lastRefreshTimes.getOrDefault(namespaceId, 0L);
  }

  /**
   * Initiates a manual refresh for the specified namespace. If a refresh service exists for the
   * namespace, triggers an unforced manual refresh. If no refresh service exists, adds a new
   * refresh service and starts it. This is triggered when the user calls the list API for namespace
   * and repository source control metadata.
   *
   * @param namespaceId The ID of the namespace for which to run the refresh service.
   */
  public void runRefreshService(NamespaceId namespaceId)
      throws NotFoundException {
    lastRefreshTimes.putIfAbsent(namespaceId, createRefreshService(namespaceId).refreshMetadata());
  }

  public RepositoryMeta getRepositoryMeta(NamespaceId namespace)
      throws RepositoryNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new RepositoryTable(context);
  }
}
