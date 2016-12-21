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

package co.cask.cdap.internal.app.runtime.impersonation;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.ImpersonationInfoNotFound;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This class manages impersonation info (such as the list of principals and keytabs), as well as metadata
 * for each entity (such as namespace, application, dataset, etc.) that needs impersonation.
 *
 * TODO: talk about mutability
 * TODO: talk about who is responsible for creating/deleting the associations
 *
 * Currently, there are two types of information being maintained:
 *   1) An ImpersonationInfo can be stored, which maintains a mapping from principal to the URI of its keytab
 *   2) A mapping can be stored (EntityId --> principal), which determines which principal should be used for operations
 *      involving that entity.
 *
 * Adding an {@link ImpersonationInfo} creates a row with format:
 * rowkey i:{principal}, column {'c'}, and keytabURI as the value
 *
 * Deploying a namespace configured with impersonation, creates an association in the form:
 * rowkey a:{EntityId#toString}, column {'c'}, and the principal as the value
 *
 * TODO: this point down
 * For example, suppose we add a system artifact etlbatch-3.1.0, which contains an ETLBatch application class.
 * The meta table will look like:
 *
 * rowkey                            columns
 * a:system:ETLBatch                 etlbatch:3.1.0 -> {AppData}
 * r:system:etlbatch                 3.1.0 -> {ArtifactData}
 *
 * After that, a system artifact etlbatch-lib-3.1.0 is added, which extends etlbatch and contains
 * stream sink and table sink plugins. The meta table will look like:
 *
 * rowkey                            columns
 * a:system:ETLBatch                 etlbatch:3.1.0 -> {AppData}
 * p:system:etlbatch:sink:stream     system:etlbatch-lib:3.1.0 -> {PluginData}
 * p:system:etlbatch:sink:table      system:etlbatch-lib:3.1.0 -> {PluginData}
 * r:system:etlbatch                 3.1.0 -> {ArtifactData}
 * r:system:etlbatch-lib             3.1.0 -> {ArtifactData}
 *
 * With this schema we can perform a scan to look up AppClasses, a scan to look up plugins that extend a specific
 * artifact, and a scan to look up artifacts.
 */
public class ImpersonationStore {
  private static final String IMPERSONATION_INFO_PREFIX = "i";
  private static final String ASSOCIATION_PREFIX = "a";
  // currently, we only leverage one column of the table. However, not using KeyValueTable, so that being able to use
  // additional columns in the future is simple
  private static final byte[] COL = Bytes.toBytes("c");
  private static final DatasetId META_ID = NamespaceId.SYSTEM.dataset("impersonation.meta");
  private static final DatasetProperties META_PROPERTIES =
    DatasetProperties.builder().add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.COLUMN.name()).build();

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  ImpersonationStore(DatasetFramework datasetFramework,
                     TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, META_ID.getParent(),
                                                                   Collections.<String, String>emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by impersonation store.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), META_ID, META_PROPERTIES);
  }

  private Table getMetaTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, META_ID, Table.class.getName(), META_PROPERTIES);
  }

  @VisibleForTesting
  void clear() throws Exception {
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        // drop all rows
        Table metaTable = getMetaTable(context);
        try (Scanner scan = metaTable.scan(null, null)) {
          Row row;
          while ((row = scan.next()) != null) {
            metaTable.delete(row.getRow());
          }
        }
      }
    });
  }

  public void createAssociation(final EntityId entityId, final String principal) throws IOException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Table metaTable = getMetaTable(context);
          byte[] keytabURI = metaTable.get(createRowKey(principal)).get(COL);
          // just check that there is an ImpersonationInfo associated with the principal we're adding an association for
          if (keytabURI == null) {
            throw new IllegalArgumentException(
              String.format("Attempted to add an association for entity '%s' to principal '%s' " +
                              "which does not have a keytab associated with it.",
                            entityId, principal));
          }
  // TODO: check if already exists, and throw AlreadyExistsException? Maybe not, if the callers need it to be idempotent
          metaTable.put(createRowKey(entityId),
                        COL, Bytes.toBytes(principal));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  public ImpersonationInfo getAssociation(final EntityId entityId) throws IOException, NotFoundException {
    try {
      ImpersonationInfo impersonationInfo = Transactions.execute(transactional, new TxCallable<ImpersonationInfo>() {
        @Override
        public ImpersonationInfo call(DatasetContext context) throws Exception {
          byte[] principalBytes =
            getMetaTable(context).get(createRowKey(entityId), COL);
          if (principalBytes == null) {
            return null;
          }
          String principal = Bytes.toString(principalBytes);
          byte[] keytabURIBytes = getMetaTable(context).get(createRowKey(principal), COL);
          // this shouldn't happen, if principalBytes is not null
          Preconditions.checkNotNull(keytabURIBytes,
                                     String.format("Found no keytab URI for principal %s, associated with entity %s.",
                                                   principal, entityId));
          return new ImpersonationInfo(principal, Bytes.toString(keytabURIBytes));
        }
      });
      if (impersonationInfo == null) {
        throw new NotFoundException(String.format("Impersonation information not found for entity '%s'.", entityId));
      }
      return impersonationInfo;
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  // idempotent
  public void deleteAssociation(final EntityId entityId) throws IOException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getMetaTable(context).delete(createRowKey(entityId));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  // TODO: introduce a listAssociations, for debugging

  // creates rowkey for association entries
  private byte[] createRowKey(EntityId entityId) {
    return Bytes.toBytes(ASSOCIATION_PREFIX + ':' + entityId.toString());
  }

  public void addImpersonationInfo(final ImpersonationInfo impersonationInfo) throws IOException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Table metaTable = getMetaTable(context);
          // TODO: check if already exists, and throw AlreadyExistsException?
          metaTable.put(createRowKey(impersonationInfo.getPrincipal()),
                        COL, Bytes.toBytes(impersonationInfo.getKeytabURI()));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  public ImpersonationInfo getImpersonationInfo(final String principal) throws IOException, ImpersonationInfoNotFound {
    try {
      ImpersonationInfo impersonationInfo = Transactions.execute(transactional, new TxCallable<ImpersonationInfo>() {
        @Override
        public ImpersonationInfo call(DatasetContext context) throws Exception {
          byte[] keytabURIBytes = getMetaTable(context).get(createRowKey(principal), COL);
          if (keytabURIBytes == null) {
            // throw the NotFoundException outside of this Callable, so it doesn't get wrapped
            return null;
          }
          return new ImpersonationInfo(principal, Bytes.toString(keytabURIBytes));
        }
      });
      if (impersonationInfo == null) {
        throw new ImpersonationInfoNotFound(principal);
      }
      return impersonationInfo;
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  // idempotent
  public void delete(final String principal) throws IOException {
    // TODO: enforce that there are no associations from entity to this principal (implement w/ counter?)
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getMetaTable(context).delete(createRowKey(principal));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  public List<ImpersonationInfo> listImpersonationInfos() throws IOException {
    try {
      return Transactions.execute(transactional, new TxCallable<List<ImpersonationInfo>>() {
        @Override
        public List<ImpersonationInfo> call(DatasetContext context) throws Exception {
          List<ImpersonationInfo> impersonationInfos = Lists.newArrayList();
          // no need to recreate this object each time (cache in some field, if its commonly used)
          Scan scan = new Scan(Bytes.toBytes(String.format("%s:", IMPERSONATION_INFO_PREFIX)),
                               Bytes.toBytes(String.format("%s;", IMPERSONATION_INFO_PREFIX)));
          try (Scanner scanner = getMetaTable(context).scan(scan)) {
            Row row;
            while ((row = scanner.next()) != null) {
              impersonationInfos.add(parseImpersonationInfo(row));
            }
          }
          return Collections.unmodifiableList(impersonationInfos);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, IOException.class);
    }
  }

  private ImpersonationInfo parseImpersonationInfo(Row row) throws IOException {
    String key = Bytes.toString(row.getRow());
    Iterator<String> parts = Splitter.on(':').limit(2).split(key).iterator();
    // first part is the IMPERSONATION_INFO_PREFIX
    parts.next();
    // next is principal
    String principal = parts.next();

    // shouldn't be null, but just in case...
    byte[] bytes = Preconditions.checkNotNull(row.get(COL));
    return new ImpersonationInfo(principal, Bytes.toString(bytes));
  }

  // creates rowkey for ImpersonationInfo entries
  private byte[] createRowKey(String principal) {
    return Bytes.toBytes(Joiner.on(':').join(IMPERSONATION_INFO_PREFIX, principal));
  }
}
