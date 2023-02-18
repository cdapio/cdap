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

package io.cdap.cdap.store;

import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import javax.inject.Inject;

/**
 * The default implementation of {@link RepositoryStore}
 */
public class DefaultRepositoryStore implements RepositoryStore {

  private final TransactionRunner transactionRunner;

  @Inject
  public DefaultRepositoryStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context) throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  private NamespaceTable getNamespaceTable(StructuredTableContext context) throws TableNotFoundException {
    return new NamespaceTable(context);
  }

  @Override
  public RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository)
    throws NamespaceNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable nsTable = getNamespaceTable(context);
      if (nsTable.get(namespace) == null) {
        throw new NamespaceNotFoundException(namespace);
      }

      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.create(namespace, repository);
      return repoTable.get(namespace);
    }, NamespaceNotFoundException.class);
  }

  @Override
  public void deleteRepository(NamespaceId namespace) {
    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.delete(namespace);
    });
  }

  @Override
  public RepositoryMeta getRepositoryMeta(NamespaceId namespace) throws RepositoryNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }
}
