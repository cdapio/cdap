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

package io.cdap.cdap.internal.app.services;

import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.RepositoryTable;

/**
 * Service that manages source control for .
 */
public class SourceControlService {

  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final TransactionRunner transactionRunner;

  @Inject
  public SourceControlService(TransactionRunner transactionRunner,
                              AccessEnforcer accessEnforcer,
                              AuthenticationContext authenticationContext) {
    this.transactionRunner = transactionRunner;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context) throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  public void setRepository(NamespaceId namespace, RepositoryConfig repository) throws BadRequestException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (repository == null || !repository.isValid()) {
      throw new BadRequestException(String.format("Invalid repository configuration: %s.", repository));
    }

    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      table.create(namespace, repository);
    });
  }

  public void deleteRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.DELETE);
    
    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      table.delete(namespace);
    });
  }

  public RepositoryConfig getRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);
    
    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      return table.get(namespace);
    });
  }
}
