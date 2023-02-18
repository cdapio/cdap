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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlConfig;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.NamespaceTable;
import io.cdap.cdap.store.RepositoryTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Service that manages source control for repositories and applications.
 * It exposes repository CRUD apis and source control tasks that do pull/pull/list applications in linked repository.
 */
public class SourceControlManagementService {
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final TransactionRunner transactionRunner;
  private final CConfiguration cConf;
  private final SecureStore secureStore;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final ApplicationLifecycleService appLifecycleService;
  private final Store store;


  @Inject
  public SourceControlManagementService(CConfiguration cConf,
                                        SecureStore secureStore,
                                        TransactionRunner transactionRunner,
                                        AccessEnforcer accessEnforcer,
                                        AuthenticationContext authenticationContext,
                                        SourceControlOperationRunner sourceControlOperationRunner,
                                        ApplicationLifecycleService applicationLifecycleService,
                                        Store store) {
    this.cConf = cConf;
    this.secureStore = secureStore;
    this.transactionRunner = transactionRunner;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.appLifecycleService = applicationLifecycleService;
    this.store = store;
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context) throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  private NamespaceTable getNamespaceTable(StructuredTableContext context) throws TableNotFoundException {
    return new NamespaceTable(context);
  }

  public RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository)
    throws NamespaceNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.UPDATE);

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

  public void deleteRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.DELETE);

    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.delete(namespace);
    });
  }

  public RepositoryMeta getRepositoryMeta(NamespaceId namespace) throws RepositoryNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);

    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }

  public void validateRepository(NamespaceId namespace, RepositoryConfig repoConfig) {
    try {
      RepositoryManager.validateConfig(secureStore, new SourceControlConfig(namespace, repoConfig, cConf));
    } catch (IOException e) {
      // TODO: CDAP-20354, throw correct non-400 validation errors
      throw new RepositoryConfigValidationException("Internal error: " + e.getMessage(), e);
    }
  }

  public PushAppsResponse pushApps(NamespaceId namespace, List<ApplicationId> appIds, @Nullable String commitMessage)
    throws Exception {
    RepositoryConfig repoConfig = getRepositoryMeta(namespace).getConfig();
    Map<ApplicationId, ApplicationDetail> details = getAndValidateAppDetails(appIds);

    String committer = authenticationContext.getPrincipal().getName();
    List<ApplicationDetail> appsToPush = new ArrayList<>(details.values());

    // TODO CDAP-20371 revisit and put correct Author and Committer, for now they are the same
    CommitMeta commitMeta = new CommitMeta(committer, committer, System.currentTimeMillis(), commitMessage);

    // TODO: CDAP-20360, add namespace in RepositoryConfig so that we don't need to further pass it in OperationRunner
    PushAppsResponse pushResponse = sourceControlOperationRunner.push(namespace, repoConfig, appsToPush, commitMeta);
    updateSourceControlMeta(namespace, pushResponse);

    return pushResponse;
  }

  private void updateSourceControlMeta(NamespaceId namespace, PushAppsResponse pushResponse) {
    Map<ApplicationId, SourceControlMeta> pushAppsMeta = pushResponse.getApps().stream()
      .collect(Collectors.toMap(
        entry -> namespace.app(entry.getName(), entry.getVersion()),
        entry -> new SourceControlMeta(entry.getFileHash())
      ));
    store.setAppSourceControlMetas(pushAppsMeta);
  }

  private Map<ApplicationId, ApplicationDetail> getAndValidateAppDetails(List<ApplicationId> appIds) throws Exception {
    // TODO: CDAP-20382 write into disk instead of loading into memory
    Map<ApplicationId, ApplicationDetail> details = appLifecycleService.getAppDetails(appIds, false);

    // Throw NotFoundException when applications in request is not found.
    // We don't do partial push and don't send partial result
    Set<ApplicationId> appsNotFound = Sets.difference(new HashSet<>(appIds), details.keySet());
    if (!appsNotFound.isEmpty()) {
      String notFoundMessage = appsNotFound.stream().map(EntityId::toString)
        .collect(Collectors.joining(", ", "Applications ", " not found."));
      throw new NotFoundException(notFoundMessage);
    }
    
    return details;
  }
}
