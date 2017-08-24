/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.PartitionNotFoundException;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionAlreadyExistsException;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionMetadata;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.Partitioning.FieldType;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.data.RuntimeProgramContext;
import co.cask.cdap.data.RuntimeProgramContextAware;
import co.cask.cdap.data2.dataset2.lib.file.FileSetDataset;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.id.DatasetId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Provider;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class PartitionedFileSetDataset extends AbstractDataset
  implements PartitionedFileSet, DatasetOutputCommitter, RuntimeProgramContextAware {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetDataset.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(1)));
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec()).create();
  private static final String QUARANTINE_DIR = ".quarantine";

  // A function to convert PartitionOperation into PartitionKey by getting the key from the operation.
  private static final Function<PartitionOperation, PartitionKey> OP_TO_KEY =
    new Function<PartitionOperation, PartitionKey>() {
      @Override
      public PartitionKey apply(PartitionOperation operation) {
        return operation.getPartitionKey();
      }
    };

  /**
   * A key that is guaranteed to be in the input format in configuration, where the value represents a GSON-serialized
   * mapping from a partition's path to its partition key.
   */
  public static final String PATH_TO_PARTITIONING_MAPPING = "path.to.partition.mapping";

  // column keys
  protected static final byte[] WRITE_PTR_COL = { 'w' };
  protected static final byte[] CREATION_TIME_COL = { 'c' };
  private static final byte[] LAST_MODIFICATION_TIME_COL = { 'm' };
  private static final byte[] RELATIVE_PATH = { 'p' };
  private static final byte[] METADATA_PREFIX = { 'm', '.' };

  protected final FileSet files;
  protected final DatasetSpecification spec;
  protected final boolean isExternal;
  private final boolean exploreEnabled;
  protected final Map<String, String> runtimeArguments;
  protected final Partitioning partitioning;
  private final IndexedTable partitionsTable;
  private final Provider<ExploreFacade> exploreFacadeProvider;

  private final DatasetId datasetInstanceId;

  // Keep track of all partitions' being added/dropped in this transaction, so we can rollback their paths,
  // if necessary.
  private final List<PartitionOperation> operationsInThisTx = new ArrayList<>();

  private Transaction tx;
  private RuntimeProgramContext runtimeProgramContext;

  // this will store the result of getInputKeys() after it is called (the result is needed by
  // both getInputFormat() and getInputFormatConfiguration(), and we don't want to compute it twice).
  private AtomicReference<Collection<PartitionKey>> inputKeysCache = null;

  public PartitionedFileSetDataset(DatasetContext datasetContext, String name,
                                   Partitioning partitioning, FileSet fileSet, IndexedTable partitionTable,
                                   DatasetSpecification spec, Map<String, String> arguments,
                                   Provider<ExploreFacade> exploreFacadeProvider) {
    super(name, partitionTable);
    this.files = fileSet;
    this.partitionsTable = partitionTable;
    this.spec = spec;
    this.isExternal = FileSetProperties.isDataExternal(spec.getProperties());
    this.exploreEnabled = FileSetProperties.isExploreEnabled(spec.getProperties());
    this.runtimeArguments = arguments;
    this.partitioning = partitioning;
    this.exploreFacadeProvider = exploreFacadeProvider;
    this.datasetInstanceId = new DatasetId(datasetContext.getNamespaceId(), name);
  }

  @Override
  public void setContext(RuntimeProgramContext context) {
    this.runtimeProgramContext = context;
  }

  /**
   * Returns the current {@link RuntimeProgramContext} or {@code null} if it is not available.
   */
  @Nullable
  protected RuntimeProgramContext getRuntimeProgramContext() {
    return runtimeProgramContext;
  }

  @Override
  public void startTx(Transaction tx) {
    operationsInThisTx.clear();
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public boolean commitTx() throws Exception {
    if (!super.commitTx()) {
      return false;
    }

    // Publish a notification if there was new partition added.
    RuntimeProgramContext runtimeProgramContext = getRuntimeProgramContext();
    if (runtimeProgramContext != null && !operationsInThisTx.isEmpty()) {
      List<PartitionKey> newPartitions = Lists.newArrayList(
        Iterables.transform(Iterables.filter(operationsInThisTx, AddPartitionOperation.class), OP_TO_KEY));

      // Only fire notification if there is new partition added
      if (!newPartitions.isEmpty()) {
        runtimeProgramContext.notifyNewPartitions(newPartitions);
      }
    }
    return true;
  }

  @Override
  public void postTxCommit() {
    // simply delete the quarantine directory for this transaction
    try {
      Location quarantine = getQuarantineLocation();
      if (quarantine.exists()) {
        boolean deleteSuccess = quarantine.delete(true);
        if (!deleteSuccess) {
          throw new DataSetException(String.format("Error deleting quarantine location %s.", quarantine));
        }
      }
    } catch (IOException e) {
      throw new DataSetException(String.format("Error deleting quarantine location for tx %s.", tx.getWritePointer()),
                                 e);
    }

    this.tx = null;
    super.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    rollbackPartitionOperations();
    this.tx = null;
    return super.rollbackTx();
  }

  private void rollbackPartitionOperations() throws Exception {
    // rollback all the partition add and drop operations, in reverse order
    // if any throw exception, suppress it temporarily while attempting to roll back the remainder operations
    Exception caughtExn = null;
    for (int i = operationsInThisTx.size() - 1; i >= 0; i--) {
      PartitionOperation operation = operationsInThisTx.get(i);
      try {
        rollbackOperation(operation);
      } catch (Exception e) {
        if (caughtExn == null) {
          caughtExn = e;
        } else {
          caughtExn.addSuppressed(e);
        }
      }
    }
    operationsInThisTx.clear();
    if (caughtExn != null) {
      throw caughtExn;
    }
  }

  private void rollbackOperation(PartitionOperation operation) throws Exception {
    if (operation instanceof AddPartitionOperation) {
      undoPartitionCreate((AddPartitionOperation) operation);
    } else if (operation instanceof DropPartitionOperation) {
      undoPartitionDelete((DropPartitionOperation) operation);
    } else {
      // this shouldn't happen, since we only have the above subclasses
      throw new IllegalArgumentException("Unknown operation: " + operation.getClass().getSimpleName());
    }
  }

  private void undoPartitionDelete(DropPartitionOperation operation) throws Exception {
    // move from quarantine, back to original location
    Location srcLocation = getQuarantineLocation().append(operation.getRelativePath());
    if (srcLocation.exists()) {
      srcLocation.renameTo(files.getLocation(operation.getRelativePath()));
    }
    // recreating the partition in Hive only makes sense if the rename succeeds
    addPartitionToExplore(operation.getPartitionKey(), operation.getRelativePath());
  }

  private void undoPartitionCreate(AddPartitionOperation operation) throws Exception {
    Exception caughtExn = null;
    if (operation.isExplorePartitionCreated()) {
      try {
        dropPartitionFromExplore(operation.getPartitionKey());
      } catch (Exception e) {
        caughtExn = e;
      }
    }
    if (operation.isFilesCreated()) {
      try {
        Location location = files.getLocation(operation.getRelativePath());
        if (location.exists() && !location.delete(true)) {
          throw new IOException(String.format("Failed to delete location %s.", location));
        }
      } catch (Exception e) {
        if (caughtExn != null) {
          caughtExn.addSuppressed(e);
        } else {
          caughtExn = e;
        }
      }
    }
    if (caughtExn != null) {
      throw caughtExn;
    }
  }

  private Location getQuarantineLocation() throws IOException {
    // each transaction must not share its quarantine directory with another transaction
    return files.getBaseLocation().append(QUARANTINE_DIR + "." + tx.getTransactionId());
  }

  @Override
  public Partitioning getPartitioning() {
    return partitioning;
  }

  @WriteOnly
  @Override
  public void addPartition(PartitionKey key, String path) {
    addPartition(key, path, Collections.<String, String>emptyMap());
  }

  @WriteOnly
  @Override
  public void addPartition(PartitionKey key, String path, Map<String, String> metadata) {
    // how do we know that filesCreated=false?
    addPartition(key, path, metadata, false, false);
  }

  public void addPartition(PartitionKey key, String path, Map<String, String> metadata, boolean filesCreated,
                           boolean allowAppend) {
    byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    boolean appending = !row.isEmpty();
    if (appending && !allowAppend) {
      throw new PartitionAlreadyExistsException(getName(), key);
    }
    if (appending) {
      // this can happen if user originally created the partition with a custom relative path
      String existingPath = Bytes.toString(row.get(RELATIVE_PATH));
      if (!path.equals(existingPath)) {
        throw new DataSetException(String.format("Attempting to append to Dataset '%s', to partition '%s' with a " +
                                                   "different path. Original path: '%s'. New path: '%s'",
                                                 getName(), key.toString(), existingPath, path));
      }
    }
    LOG.debug("{} partition with key {} and path {} to dataset {}", appending ? "Appending to" : "Creating",
              key, path, getName());
    AddPartitionOperation operation = new AddPartitionOperation(key, path, filesCreated);
    operationsInThisTx.add(operation);

    Put put = new Put(rowKey);
    byte[] nowInMillis = Bytes.toBytes(System.currentTimeMillis());
    if (!appending) {
      put.add(RELATIVE_PATH, Bytes.toBytes(path));
      put.add(CREATION_TIME_COL, nowInMillis);
    }
    put.add(LAST_MODIFICATION_TIME_COL, nowInMillis);

    // we allow updates, because an update will only happen if its an append
    addMetadataToPut(row, metadata, put, true);
    // index each row by its transaction's write pointer
    put.add(WRITE_PTR_COL, tx.getWritePointer());

    partitionsTable.put(put);

    if (!appending) {
      addPartitionToExplore(key, path);
      operation.setExplorePartitionCreated();
    }
  }

  @ReadWrite
  @Override
  public PartitionConsumerResult consumePartitions(PartitionConsumerState partitionConsumerState) {
    return consumePartitions(partitionConsumerState, Integer.MAX_VALUE, new Predicate<PartitionDetail>() {
      @Override
      public boolean apply(@Nullable PartitionDetail input) {
        return true;
      }
    });
  }

  /**
   * While applying a partition filter and a limit, parse partitions from the rows of a scanner and add them to a list.
   * Note that multiple partitions can have the same transaction write pointer. For each set of partitions with the same
   * write pointer, we either add the entire set or exclude the entire set. The limit is applied after adding each such
   * set of partitions to the list.
   *
   * @param scanner the scanner on the partitions table from which to read partitions
   * @param partitions list to add the qualifying partitions to
   * @param limit limit, which once reached, partitions committed by other transactions will not be added.
   *              The limit is checked after adding consuming all partitions of a transaction, so
   *              the total number of consumed partitions may be greater than this limit.
   * @param predicate predicate to apply before adding to the partitions list
   * @return Transaction ID of the partition that we reached in the scanner, but did not add to the list. This value
   *         can be useful in future scans.
   */
  @Nullable
  private Long scannerToPartitions(Scanner scanner, List<PartitionDetail> partitions, int limit,
                                   Predicate<PartitionDetail> predicate) {
    Long prevTxId = null;
    Row row;
    while ((row = scanner.next()) != null) {
      PartitionKey key = parseRowKey(row.getRow(), partitioning);

      String relativePath = Bytes.toString(row.get(RELATIVE_PATH));
      Long txId = Bytes.toLong(row.get(WRITE_PTR_COL));

      // if we are on a partition written by a different transaction, check if we are over the limit
      // we don't want to do a check on every partition because we want to either add all partitions written
      // by a transaction or none, since we keep our marker based upon transaction id.
      if (prevTxId != null && !prevTxId.equals(txId)) {
        if (partitions.size() >= limit) {
          return txId;
        }
      }
      prevTxId = txId;

      BasicPartitionDetail partitionDetail =
        new BasicPartitionDetail(PartitionedFileSetDataset.this, relativePath, key, metadataFromRow(row));

      if (!predicate.apply(partitionDetail)) {
        continue;
      }
      partitions.add(partitionDetail);
    }
    return null;
  }

  // PartitionConsumerState consists of two things:
  //   1) A list of transaction IDs representing the list of transactions in progress during the previous call.
  //      Each of these transaction IDs need to be checked for new partitions because there may be partitions created by
  //      those partitions since the previous call.
  //   2) A transaction ID from which to start scanning for new partitions. This is an exclusive end range that the
  //      previous call stopped scanning partitions at.
  //   Note that each of the transactions IDs in (1) will be smaller than the transactionId in (2).
  @ReadWrite
  @Override
  public PartitionConsumerResult consumePartitions(PartitionConsumerState partitionConsumerState, int limit,
                                                   Predicate<PartitionDetail> predicate) {
    List<Long> previousInProgress = partitionConsumerState.getVersionsToCheck();
    Set<Long> noLongerInProgress = setDiff(previousInProgress, tx.getInProgress());

    List<PartitionDetail> partitions = Lists.newArrayList();

    Iterator<Long> iter = noLongerInProgress.iterator();
    while (iter.hasNext()) {
      Long txId = iter.next();
      if (partitions.size() >= limit) {
        break;
      }
      try (Scanner scanner = partitionsTable.readByIndex(WRITE_PTR_COL, Bytes.toBytes(txId))) {
        scannerToPartitions(scanner, partitions, limit, predicate);
      }
      // remove the txIds as they are added to the partitions list already
      // if they're not removed, they will be persisted in the state for the next scan
      iter.remove();
    }

    // exclusive scan end, to be used as the start for a next call to consumePartitions
    long scanUpTo;
    if (partitions.size() < limit) {
      // no read your own writes (partitions)
      scanUpTo = Math.min(tx.getWritePointer(), tx.getReadPointer() + 1);
      Long endTxId;
      try (Scanner scanner = partitionsTable.scanByIndex(WRITE_PTR_COL,
                                                         Bytes.toBytes(partitionConsumerState.getStartVersion()),
                                                         Bytes.toBytes(scanUpTo))) {
        endTxId = scannerToPartitions(scanner, partitions, limit, predicate);
      }
      if (endTxId != null) {
        // nonnull means that the scanner was not exhausted
        scanUpTo = endTxId;
      }
    } else {
      // if we have already hit the limit, don't scan; instead, use the startVersion as the startVersion to the next
      // call to consumePartitions
      scanUpTo = partitionConsumerState.getStartVersion();
    }

    List<Long> inProgressBeforeScanEnd = Lists.newArrayList(noLongerInProgress);
    for (long txId : tx.getInProgress()) {
      if (txId >= scanUpTo) {
        break;
      }
      inProgressBeforeScanEnd.add(txId);
    }
    return new PartitionConsumerResult(new PartitionConsumerState(scanUpTo, inProgressBeforeScanEnd),
                                       partitions);
  }

  // returns the set of Longs that are in oldLongs, but not in newLongs (oldLongs - newLongs)
  private Set<Long> setDiff(List<Long> oldLongs, long[] newLongs) {
    Set<Long> oldLongsSet = new HashSet<>(oldLongs);
    for (long newLong : newLongs) {
      oldLongsSet.remove(newLong);
    }
    return oldLongsSet;
  }

  @WriteOnly
  @Override
  public void addMetadata(PartitionKey key, String metadataKey, String metadataValue) {
    addMetadata(key, ImmutableMap.of(metadataKey, metadataValue));
  }

  @WriteOnly
  @Override
  public void addMetadata(PartitionKey key, Map<String, String> metadata) {
    setMetadata(key, metadata, false);
  }

  @Override
  public void setMetadata(PartitionKey key, Map<String, String> metadata) {
    setMetadata(key, metadata, true);
  }

  private void setMetadata(PartitionKey key, Map<String, String> metadata, boolean allowUpdates) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (row.isEmpty()) {
      throw new PartitionNotFoundException(key, getName());
    }

    Put put = new Put(rowKey);
    addMetadataToPut(row, metadata, put, allowUpdates);
    partitionsTable.put(put);
  }

  private void addMetadataToPut(Row existingRow, Map<String, String> metadata, Put put,
                                boolean allowUpdates) {
    // ensure that none of the entries already exist in the metadata
    if (!allowUpdates) {
      checkMetadataDoesNotExist(existingRow, metadata);
    }
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      byte[] columnKey = columnKeyFromMetadataKey(entry.getKey());
      put.add(columnKey, Bytes.toBytes(entry.getValue()));
    }
  }

  private void checkMetadataDoesNotExist(Row existingRow, Map<String, String> metadata) {
    if (existingRow.isEmpty()) {
      return;
    }
    for (String metadataKey : metadata.keySet()) {
      byte[] columnKey = columnKeyFromMetadataKey(metadataKey);
      if (existingRow.get(columnKey) != null) {
        throw new DataSetException(String.format("Entry already exists for metadata key: %s", metadataKey));
      }
    }
  }

  @Override
  public void removeMetadata(PartitionKey key, String metadataKey) {
    removeMetadata(key, Collections.singleton(metadataKey));
  }

  @Override
  public void removeMetadata(PartitionKey key, Set<String> metadataKeys) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (row.isEmpty()) {
      throw new PartitionNotFoundException(key, getName());
    }

    int i = 0;
    byte[][] deleteColumns = new byte[metadataKeys.size()][];
    for (String metadataKey : metadataKeys) {
      deleteColumns[i++] = columnKeyFromMetadataKey(metadataKey);
    }
    partitionsTable.delete(rowKey, deleteColumns);
  }

  @VisibleForTesting
  public void addPartitionToExplore(PartitionKey key, String path) {
    if (exploreEnabled) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.addPartition(datasetInstanceId, spec, key, files.getLocation(path).toURI().getPath());
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to add partition for key %s with path %s to explore table.", key.toString(), path), e);
        }
      }
    }
  }

  @WriteOnly
  @Override
  public void dropPartition(PartitionKey key) {
    byte[] rowKey = generateRowKey(key, partitioning);
    PartitionDetail partition = getPartition(key);
    if (partition == null) {
      // silently ignore non-existing partitions
      return;
    }
    // TODO: make DDL operations transactional [CDAP-1393]
    dropPartitionFromExplore(key);
    partitionsTable.delete(rowKey);
    if (!isExternal) {
      Location partitionLocation = partition.getLocation();
      try {
        if (partitionLocation.exists()) {
          Location dstLocation = getQuarantineLocation().append(partition.getRelativePath());
          Location dstParent = Locations.getParent(dstLocation);
          // shouldn't be null, since dstLocation was created by appending to a location, so it must have a parent
          Preconditions.checkNotNull(dstParent);
          // before moving into quarantine, we need to ensure that parent location exists
          if (!dstParent.exists()) {
            if (!dstParent.mkdirs()) {
              throw new DataSetException(String.format("Failed to create parent directory %s", dstParent));
            }
          }
          partitionLocation.renameTo(dstLocation);
        }
      } catch (IOException ioe) {
        throw new DataSetException(String.format("Failed to move location %s into quarantine", partitionLocation));
      }
      operationsInThisTx.add(new DropPartitionOperation(key, partition.getRelativePath()));
    }
  }

  private void dropPartitionFromExplore(PartitionKey key) {
    if (exploreEnabled) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.dropPartition(datasetInstanceId, spec, key);
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to drop partition for key %s from explore table.", key.toString()), e);
        }
      }
    }
  }

  @ReadWrite
  @Override
  public Future<Void> concatenatePartition(PartitionKey key) {
    PartitionDetail partition = getPartition(key);
    if (partition == null) {
      throw new PartitionNotFoundException(key, getName());
    }
    try {
      if (exploreEnabled) {
        return exploreFacadeProvider.get().concatenatePartition(datasetInstanceId, spec, key);
      } else {
        return Futures.immediateFuture(null);
      }
    } catch (Exception e) {
      throw new DataSetException(String.format(
        "Unable to concatenate partition for key %s from explore table.", key.toString()), e);
    }
  }

  @ReadOnly
  @Override
  public PartitionOutput getPartitionOutput(PartitionKey key) {
    checkNotExternal();
    assertNotExists(key, true);
    return new BasicPartitionOutput(this, getOutputPath(key), key);
  }


  // Throws PartitionAlreadyExistsException if the partition key already exists.
  // Otherwise, returns the rowkey corresponding to the PartitionKey.
  @ReadOnly
  byte[] assertNotExists(PartitionKey key, boolean supportNonTransactional) {
    byte[] rowKey = generateRowKey(key, partitioning);
    if (tx == null && supportNonTransactional) {
      if (LOG.isWarnEnabled()) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
          sb.append("\n\tat ").append(stackTraceElement.toString());
        }
        SAMPLING_LOG.warn("Operation should be performed within a transaction. " +
                            "This operation may require a transaction in the future. {}", sb);
      }
      // to handle backwards compatibility (user might have called PartitionedFileSet#getPartitionOutput outside
      // of a transaction), we can't check partition existence via the partitionsTable. As an fallback approach,
      // check the filesystem.
      Location partitionLocation = files.getLocation(getOutputPath(key));
      if (exists(partitionLocation)) {
        throw new DataSetException(String.format("Location %s for partition key %s already exists: ",
                                                 partitionLocation, key));
      }
    } else {
      Row row = partitionsTable.get(rowKey);
      if (!row.isEmpty()) {
        throw new PartitionAlreadyExistsException(getName(), key);
      }
    }
    return rowKey;
  }

  private boolean exists(Location location) {
    try {
      return location.exists();
    } catch (IOException e) {
      throw new DataSetException(e);
    }
  }

  @ReadOnly
  @Override
  public PartitionDetail getPartition(PartitionKey key) {
    byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (row.isEmpty()) {
      return null;
    }

    byte[] pathBytes = row.get(RELATIVE_PATH);
    if (pathBytes == null) {
      return null;
    }

    return new BasicPartitionDetail(this, Bytes.toString(pathBytes), key, metadataFromRow(row));
  }

  @ReadOnly
  @Override
  public Set<PartitionDetail> getPartitions(@Nullable PartitionFilter filter) {
    final Set<PartitionDetail> partitionDetails = Sets.newHashSet();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
        // metadata won't be null, because we pass 'true' as the 'decodeMetadata' parameter of 'getPartitions'
        partitionDetails.add(new BasicPartitionDetail(PartitionedFileSetDataset.this, path, key, metadata));
      }
    }, true);
    return partitionDetails;
  }

  private Collection<PartitionKey> getPartitionPaths(@Nullable PartitionFilter filter) {
    // this avoids constructing the Partition object for every partition.
    final Set<PartitionKey> partitionKeys = new HashSet<>();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
        partitionKeys.add(key);
      }
    }, false);
    return partitionKeys;
  }

  protected void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer) {
    // by default, parse the metadata from the rows
    getPartitions(filter, consumer, true);
  }

  // if decodeMetadata is false, null is passed as the PartitionMetadata to the PartitionConsumer,
  // for efficiency reasons, since the metadata is not always needed
  protected void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer, boolean decodeMetadata) {
    byte[] startKey = generateStartKey(filter);
    byte[] endKey = generateStopKey(filter);
    getPartitions(filter, consumer, decodeMetadata, startKey, endKey, Long.MAX_VALUE);
  }

  private void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer, boolean decodeMetadata,
                             @Nullable byte[] startKey, @Nullable byte[] endKey, long limit) {
    long count = 0L;
    try (Scanner scanner = partitionsTable.scan(startKey, endKey)) {
      while (count < limit) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        PartitionKey key;
        try {
          key = parseRowKey(row.getRow(), partitioning);
        } catch (IllegalArgumentException e) {
          LOG.debug(String.format("Failed to parse row key for partitioned file set '%s': %s",
                                  getName(), Bytes.toStringBinary(row.getRow())));
          continue;
        }
        if (filter != null && !filter.match(key)) {
          continue;
        }
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          consumer.consume(key, Bytes.toString(pathBytes), decodeMetadata ? metadataFromRow(row) : null);
        }
        count++;
      }
      if (count == 0) {
        warnIfInvalidPartitionFilter(filter, partitioning);
      }
    }
  }

  private PartitionMetadata metadataFromRow(Row row) {
    Map<String, String> metadata = new HashMap<>();
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      if (Bytes.startsWith(entry.getKey(), METADATA_PREFIX)) {
        String metadataKey = metadataKeyFromColumnKey(entry.getKey());
        metadata.put(metadataKey, Bytes.toString(entry.getValue()));
      }
    }

    byte[] creationTimeBytes = row.get(CREATION_TIME_COL);
    byte[] lastModificationTimeBytes = row.get(LAST_MODIFICATION_TIME_COL);
    // For backwards compatibility. In pre-4.3 CDAP, we did not write a LAST_MODIFICATION_TIME_COL. We know that such
    // partitions have not been appended to.
    if (lastModificationTimeBytes == null) {
      lastModificationTimeBytes = creationTimeBytes;
    }
    return new PartitionMetadata(metadata, Bytes.toLong(creationTimeBytes), Bytes.toLong(lastModificationTimeBytes));
  }

  private String metadataKeyFromColumnKey(byte[] columnKey) {
    return Bytes.toString(columnKey, METADATA_PREFIX.length, columnKey.length - METADATA_PREFIX.length);
  }

  private byte[] columnKeyFromMetadataKey(String metadataKey) {
    return Bytes.add(METADATA_PREFIX, Bytes.toBytes(metadataKey));
  }


  /**
   * Generate an output path for a given partition key.
   */
  // package visible for PartitionedFileSetDefinition
  String getOutputPath(PartitionKey key) {
    return getOutputPath(key, partitioning);
  }

  public static String getOutputPath(PartitionKey key, Partitioning partitioning) {
    validatePartitionKey(key, partitioning);
    StringBuilder builder = new StringBuilder();
    String sep = "";
    for (String fieldName : partitioning.getFields().keySet()) {
      builder.append(sep).append(key.getField(fieldName).toString());
      sep = "/";
    }
    return builder.toString();
  }

  /**
   * Interface use internally to build different types of results when scanning partitions.
   */
  protected interface PartitionConsumer {
    void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata);
  }

  @Override
  public void close() throws IOException {
    try {
      files.close();
    } finally {
      partitionsTable.close();
    }
  }

  @Override
  public String getInputFormatClassName() {
    Collection<PartitionKey> inputKeys = getInputKeys();
    if (inputKeys.isEmpty()) {
      return EmptyInputFormat.class.getName();
    }
    return files.getInputFormatClassName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Collection<PartitionKey> inputKeys = getInputKeys();

    List<Location> inputLocations = new ArrayList<>(inputKeys.size());
    Map<String, PartitionKey> pathToKey = new HashMap<>(inputKeys.size());
    for (PartitionKey key : inputKeys) {
      PartitionDetail partition = getPartition(key);
      String path = Objects.requireNonNull(partition).getRelativePath();
      Location partitionLocation = files.getLocation(path);
      inputLocations.add(partitionLocation);
      pathToKey.put(partitionLocation.toURI().toString(), key);
    }

    Map<String, String> inputFormatConfiguration = files.getInputFormatConfiguration(inputLocations);
    inputFormatConfiguration.put(PATH_TO_PARTITIONING_MAPPING, GSON.toJson(pathToKey));
    return inputFormatConfiguration;
  }

  /**
   * Computes and returns the input partition keys given by the partition filter - if present. Otherwise, get the list
   * of partition keys explicitly specified in the runtime arguments.
   * Stores the result in a cache and returns it.
   */
  private Collection<PartitionKey> getInputKeys() {
    if (inputKeysCache != null) {
      return inputKeysCache.get();
    }
    Collection<PartitionKey> inputKeys = computeInputKeys();
    if (inputKeys == null) {
      inputKeys = PartitionedFileSetArguments.getInputPartitionKeys(runtimeArguments);
    }
    inputKeysCache = new AtomicReference<>(inputKeys);
    return inputKeys;
  }

  /**
   * If a partition filter was specified, return the partition keys of all partitions
   * matching the filter. Otherwise return null.
   */
  @Nullable
  protected Collection<PartitionKey> computeInputKeys() {
    PartitionFilter filter;
    try {
      filter = PartitionedFileSetArguments.getInputPartitionFilter(runtimeArguments);
    } catch (Exception e) {
      throw new DataSetException("Partition filter must be correctly specified in arguments.");
    }
    if (filter == null) {
      return null;
    }
    return getPartitionPaths(filter); // never returns null
  }

  @Override
  public String getOutputFormatClassName() {
    checkNotExternal();
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    if (outputKey == null) {
      return "co.cask.cdap.internal.app.runtime.batch.dataset.partitioned.DynamicPartitioningOutputFormat";
    }
    return files.getOutputFormatClassName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    checkNotExternal();
    // copy the output properties of the embedded file set to the output arguments
    Map<String, String> outputArgs = new HashMap<>(files.getOutputFormatConfiguration());

    // we set the file set's output path in the definition's getDataset(), so there is no need to configure it again.
    // here we just want to validate that an output partition key or dynamic partitioner was specified in the arguments.
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    if (outputKey == null) {
      String dynamicPartitionerClassName = PartitionedFileSetArguments.getDynamicPartitioner(runtimeArguments);
      if (dynamicPartitionerClassName == null) {
        throw new DataSetException(
          "Either a Partition key or a DynamicPartitioner class must be given as a runtime argument.");
      }

      copyDynamicPartitionerArguments(runtimeArguments, outputArgs);

      outputArgs.put(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME,
                     files.getOutputFormatClassName());
      outputArgs.put(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET, getName());
    } else {
      assertNotExists(outputKey, true);
    }
    return ImmutableMap.copyOf(outputArgs);
  }

  private void copyDynamicPartitionerArguments(Map<String, String> fromMap, Map<String, String> toMap) {
    String dynamicPartitionerClassName = PartitionedFileSetArguments.getDynamicPartitioner(fromMap);
    DynamicPartitioner.PartitionWriteOption partitionWriteOption =
      PartitionedFileSetArguments.getDynamicPartitionerWriteOption(fromMap);
    PartitionedFileSetArguments.setDynamicPartitioner(toMap, dynamicPartitionerClassName, partitionWriteOption);

    PartitionedFileSetArguments.setDynamicPartitionerConcurrency(
      toMap, PartitionedFileSetArguments.isDynamicPartitionerConcurrencyAllowed(fromMap));

    // propagate output metadata into OutputFormatConfiguration so DynamicPartitionerOutputCommitter can assign
    // the metadata when it creates the partitions
    Map<String, String> metadata = PartitionedFileSetArguments.getOutputPartitionMetadata(fromMap);
    PartitionedFileSetArguments.setOutputPartitionMetadata(toMap, metadata);
  }

  private void checkNotExternal() {
    if (isExternal) {
      throw new UnsupportedOperationException(
        "Output is not supported for external partitioned file set '" + spec.getName() + "'");
    }
  }

  @Override
  public void onSuccess() throws DataSetException {
    String outputPath = FileSetArguments.getOutputPath(runtimeArguments);
    // If there is no output path, it is either using DynamicPartitioner or the job would have failed.
    // Either way, we can't do much here.
    if (outputPath == null) {
      return;
    }
    // its possible that there is no output key, if using the DynamicPartitioner, in which case
    // DynamicPartitioningOutputFormat is responsible for registering the partitions and the metadata
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    if (outputKey != null) {
      Map<String, String> metadata = PartitionedFileSetArguments.getOutputPartitionMetadata(runtimeArguments);
      addPartition(outputKey, outputPath, metadata, true, false);
    }

    // currently, FileSetDataset#onSuccess is a no-op, but call it, in case it does something in the future
    ((FileSetDataset) files).onSuccess();
  }

  @Override
  public void onFailure() throws DataSetException {
    try {
      rollbackPartitionOperations();

      ((FileSetDataset) files).onFailure();
    } catch (Throwable caught) {
      Throwables.propagateIfPossible(caught, DataSetException.class);
      throw new DataSetException("Unable to rollback", caught);
    }
  }

  @Override
  public FileSet getEmbeddedFileSet() {
    return files;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  private void enableExplore(boolean truncating) {
    ExploreFacade exploreFacade = exploreFacadeProvider.get();
    if (exploreFacade != null) {
      try {
        exploreFacade.enableExploreDataset(datasetInstanceId, spec, truncating);
      } catch (Exception e) {
        throw new DataSetException("Unable to enable explore", e);
      }
    }
  }

  private void disableExplore() {
    ExploreFacade exploreFacade = exploreFacadeProvider.get();
    if (exploreFacade != null) {
      try {
        exploreFacade.disableExploreDataset(datasetInstanceId);
      } catch (Exception e) {
        throw new DataSetException("Unable to enable explore", e);
      }
    }
  }

  /**
   * This method can bring a partitioned file set in sync with explore. It scans the partition table and adds
   * every partition to explore. It will start multiple transactions, processing a batch of partitions in each
   * transaction. Optionally, it can disable and re-enable explore first, that is, drop and recreate the Hive table.
   * @param transactional the Transactional for executing transactions
   * @param datasetName the name of the dataset to fix
   * @param doDisable whether to disable and re-enable explore first
   * @param partitionsPerTx how many partitions to process per transaction
   * @param verbose whether to log verbosely. If true, this will log a message for every partition; otherwise it
   *                will only log a report of how many partitions were added / could not be added.
   */
  @Beta
  @SuppressWarnings("unused")
  public static void fixPartitions(Transactional transactional, final String datasetName,
                                   boolean doDisable, final int partitionsPerTx, final boolean verbose) {

    if (doDisable) {
      try {
        transactional.execute(new TxRunnable() {
          @Override
          public void run(co.cask.cdap.api.data.DatasetContext context) throws Exception {
            PartitionedFileSetDataset pfs = context.getDataset(datasetName);
            pfs.disableExplore();
            pfs.enableExplore(true); // truncating = true, because this is like truncating
          }
        });
      } catch (TransactionFailureException e) {
        throw new DataSetException("Unable to disable and enable Explore", e.getCause());
      } catch (RuntimeException e) {
        if (e.getCause() instanceof TransactionFailureException) {
          throw new DataSetException("Unable to disable and enable Explore", e.getCause().getCause());
        }
        throw e;
      }
    }
    final AtomicReference<PartitionKey> startKey = new AtomicReference<>();
    final AtomicLong errorCount = new AtomicLong(0L);
    final AtomicLong successCount = new AtomicLong(0L);
    do {
      try {
        transactional.execute(new TxRunnable() {
          @Override
          public void run(co.cask.cdap.api.data.DatasetContext context) throws Exception {
            final PartitionedFileSetDataset pfs = context.getDataset(datasetName);
            // compute start row for the scan, reset remembered start key to null
            byte[] startRow = startKey.get() == null ? null : generateRowKey(startKey.get(), pfs.getPartitioning());
            startKey.set(null);
            PartitionConsumer consumer = new PartitionConsumer() {
              int count = 0;

              @Override
              public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
                if (count >= partitionsPerTx) {
                  // reached the limit: remember this key as the start for the next round
                  startKey.set(key);
                  return;
                }
                try {
                  pfs.addPartitionToExplore(key, path);
                  successCount.incrementAndGet();
                  if (verbose) {
                    LOG.info("Added partition {} with path {}", key, path);
                  }
                } catch (DataSetException e) {
                  errorCount.incrementAndGet();
                  if (verbose) {
                    LOG.warn(e.getMessage(), e);
                  }
                }
                count++;
              }
            };
            pfs.getPartitions(null, consumer, false, startRow, null, partitionsPerTx + 1);
          }
        });
      } catch (TransactionConflictException e) {
        throw new DataSetException("Transaction conflict while reading partitions. This should never happen. " +
                                     "Make sure that no other programs are using this dataset at the same time.");
      } catch (TransactionFailureException e) {
        throw new DataSetException("Transaction failure: " + e.getMessage(), e.getCause());
      } catch (RuntimeException e) {
        // this looks like duplication but is needed in case this is run from a worker: see CDAP-6837
        if (e.getCause() instanceof TransactionConflictException) {
          throw new DataSetException("Transaction conflict while reading partitions. This should never happen. " +
                                       "Make sure that no other programs are using this dataset at the same time.");
        } else if (e.getCause() instanceof TransactionFailureException) {
          throw new DataSetException("Transaction failure: " + e.getMessage(), e.getCause().getCause());
        } else {
          throw e;
        }
      }
    } while (startKey.get() != null); // if it is null, then we consumed less than the limit in this round -> done
    LOG.info("Added {} partitions, failed to add {} partitions.", successCount.get(), errorCount.get());
  }

  //------ private helpers below here --------------------------------------------------------------

  /**
   * Logs a warning if the partition filter contains a field that is not part of the partitioning.
   */
  private void warnIfInvalidPartitionFilter(PartitionFilter filter, Partitioning partitioning) {
    if (null == filter) {
      return;
    }
    for (Map.Entry<String, PartitionFilter.Condition<? extends Comparable>> entry : filter.getConditions().entrySet()) {
      if (!partitioning.getFields().containsKey(entry.getKey())) {
        LOG.warn("Partition filter cannot match any partitions in dataset '{}' because it contains field '{}' " +
                   "that is not a valid partitioning field", getName(), entry.getKey());
      }
    }
  }

  /**
   * Validates the partition key against the partitioning.
   */
  private static void validatePartitionKey(PartitionKey key, Partitioning partitioning) {
    if (!partitioning.getFields().keySet().equals(key.getFields().keySet())) {
      throw new IllegalArgumentException(String.format(
        "Partition key is invalid: It contains fields %s, but the partitioning requires %s",
        key.getFields().keySet(), partitioning.getFields().keySet()));
    }
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      Comparable fieldValue = key.getField(fieldName);
      if (fieldValue == null) {
        throw new IllegalArgumentException(
          String.format("Incomplete partition key: value for field '%s' is missing", fieldName));
      }
      try {
        fieldType.validate(fieldValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid partition key: Value for field '%s' is incompatible with the partitioning: %s",
          fieldName, e.getMessage()));
      }
    }
  }

  /**
   * Validates the partition key against the partitioning and gererates the row key for that partition key.
   */
  @VisibleForTesting
  static byte[] generateRowKey(PartitionKey key, Partitioning partitioning) {
    validatePartitionKey(key, partitioning);
    // validate partition key, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = partitionFields.size() - 1; // one \0 between each of the fields
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      Comparable fieldValue = key.getField(fieldName);
      byte[] bytes = FieldTypes.toBytes(fieldValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    byte[] rowKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, rowKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return rowKey;
  }

  private byte[] generateStartKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the start key
      }
      Comparable lowerValue = condition.getLower();
      if (lowerValue == null) {
        break; // this field has no lower bound; we can't include any more fields in the start key
      }
      try {
        fieldType.validate(lowerValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid partition filter: Lower bound for field '%s' is incompatible with the partitioning: %s",
          fieldName, e.getMessage()));
      }
      byte[] bytes = FieldTypes.toBytes(lowerValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    byte[] startKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, startKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return startKey;
  }

  private byte[] generateStopKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    boolean allSingleValue = true;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      Comparable upperValue = condition.getUpper();
      if (upperValue == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      try {
        fieldType.validate(upperValue);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid partition filter: Upper bound for field '%s' is incompatible with the partitioning: %s",
          fieldName, e.getMessage()));
      }
      byte[] bytes = FieldTypes.toBytes(upperValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
      if (!condition.isSingleValue()) {
        allSingleValue = false;
        break; // upper bound for this field, following fields don't matter
      }
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    if (allSingleValue) {
      totalSize++; // in this case the start and stop key are equal, we append one \1 to ensure the scan is not empty
    }
    byte[] stopKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, stopKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
      if (allSingleValue && offset == stopKey.length) {
        stopKey[offset - 1] = 1; // see above - we \1 instead of \0 at the end, to make sure scan is not empty
      }
    }
    return stopKey;
  }

  @VisibleForTesting
  static PartitionKey parseRowKey(byte[] rowKey, Partitioning partitioning) {
    PartitionKey.Builder builder = PartitionKey.builder();
    int offset = 0;
    boolean first = true;
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      if (!first) {
        if (offset >= rowKey.length) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field '%s' at offset %d " +
                            "but the end of the row key is reached.", fieldName, offset));
        }
        if (rowKey[offset] != 0) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field separator \\0 before field '%s' at offset %d " +
                            "but found byte value %x.", fieldName, offset, rowKey[offset]));
        }
        offset++;
      }
      first = false;
      int size = FieldTypes.determineLengthInBytes(rowKey, offset, fieldType);
      if (size + offset > rowKey.length) {
        throw new IllegalArgumentException(
          String.format("Invalid row key: Expecting field '%s' of type %s, " +
                          "requiring %d bytes at offset %d, but only %d bytes remain.",
                        fieldName, fieldType.name(), size, offset, rowKey.length - offset));
      }
      Comparable fieldValue = FieldTypes.fromBytes(rowKey, offset, size, fieldType);
      offset += size;
      builder.addField(fieldName, fieldValue);
    }
    if (offset != rowKey.length) {
      throw new IllegalArgumentException(
        String.format("Invalid row key: Read all fields at offset %d but %d extra bytes remain.",
                      offset, rowKey.length - offset));
    }
    return builder.build();
  }

  /**
   * Simple Implementation of PartitionOutput.
   */
  protected static class BasicPartitionOutput extends BasicPartition implements PartitionOutput {
    private Map<String, String> metadata;

    protected BasicPartitionOutput(PartitionedFileSetDataset partitionedFileSetDataset, String relativePath,
                                   PartitionKey key) {
      super(partitionedFileSetDataset, relativePath, key);
      this.metadata = new HashMap<>();
    }

    @Override
    public void addPartition() {
      partitionedFileSetDataset.addPartition(key, getRelativePath(), metadata, true, false);
    }

    @Override
    public void setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
    }
  }
}
