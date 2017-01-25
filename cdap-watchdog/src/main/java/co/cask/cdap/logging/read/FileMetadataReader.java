/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Reads meta data about log files from the log meta table.
 */
public class FileMetadataReader {
  private static final byte[] ROW_KEY_PREFIX = Bytes.toBytes(200);

  private static final Comparator<LogLocation> LOG_LOCATION_COMPARATOR = new Comparator<LogLocation>() {
    @Override
    public int compare(LogLocation o1, LogLocation o2) {
      int cmp = Longs.compare(o1.getEventTimeMs(), o2.getEventTimeMs());
      if (cmp != 0) {
        return cmp;
      }
      // when two log files have same timestamp, we order them by the file creation time
      return Longs.compare(o1.getFileCreationTimeMs(), o2.getFileCreationTimeMs());
    }
  };


  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final RootLocationFactory rootLocationFactory;
  private final Impersonator impersonator;
  private final String metaTableName;

  @Inject
  public FileMetadataReader(final DatasetFramework framework,
                            final TransactionSystemClient txClient,
                            final RootLocationFactory rootLocationFactory,
                            final Impersonator impersonator) {
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(framework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = framework;
    this.rootLocationFactory = rootLocationFactory;
    this.impersonator = impersonator;
    this.metaTableName = LoggingConfiguration.LOG_META_DATA_TABLE;
  }


  // This is only used in testing
  public FileMetadataReader(final DatasetFramework framework,
                            final TransactionSystemClient txClient,
                            final RootLocationFactory rootLocationFactory,
                            final Impersonator impersonator,
                            final String metaTableName) {
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(framework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = framework;
    this.rootLocationFactory = rootLocationFactory;
    this.impersonator = impersonator;
    this.metaTableName = metaTableName;
  }

  private Table getLogMetaTable(DatasetContext datasetContext) throws IOException, DatasetManagementException {
    DatasetId metaTableInstanceId = NamespaceId.SYSTEM.dataset(metaTableName);
    return DatasetsUtil.getOrCreateDataset(datasetContext,
                                           datasetFramework, metaTableInstanceId, Table.class.getName(),
                                           DatasetProperties.EMPTY);
  }

  private byte[] getRowKey(LogPathIdentifier logPathIdentifier) {
    return Bytes.add(ROW_KEY_PREFIX, logPathIdentifier.getRowKey().getBytes());
  }

  /**
   * Returns a list of log files for a logging context.
   *
   * @param logPathIdentifier logging context identifier.
   * @param startTimestampMs starting timestamp in milli seconds
   * @param endTimestampMs ending timestamp in milli seconds
   * @return List of {@link LogLocation}
   */
  public List<LogLocation> listFiles(final LogPathIdentifier logPathIdentifier,
                                     final long startTimestampMs, final long endTimestampMs) throws Exception {

    return txExecute(transactional, new TxCallable<List<LogLocation>>() {
      @Override
      public List<LogLocation> call(DatasetContext context) throws Exception {
        Table table = getLogMetaTable(context);
        final Row cols = table.get(getRowKey(logPathIdentifier));

        if (cols.isEmpty()) {
          //noinspection unchecked
          return new ArrayList<>();
        }

        final List<LogLocation> files = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : cols.getColumns().entrySet()) {
          // the location can be any location from on the filesystem for custom mapped namespaces
          if (entry.getKey().length == 8) {
            long eventTimestamp = Bytes.toLong(entry.getKey());
            if (!(eventTimestamp > endTimestampMs)) {
              // old format
              files.add(new LogLocation(LogLocation.VERSION_0,
                                        eventTimestamp,
                                        // use 0 as current time as this information is not available
                                        0,
                                        rootLocationFactory.create(new URI(Bytes.toString(entry.getValue()))),
                                        logPathIdentifier.getNamespaceId(), impersonator));
            }
          } else if (entry.getKey().length == 17) {
            long eventTimestamp = Bytes.toLong(entry.getKey(), 1, Bytes.SIZEOF_LONG);
            if (!(eventTimestamp > endTimestampMs)) {
              // new format
              files.add(new LogLocation(LogLocation.VERSION_1,
                                        // skip the first (version) byte
                                        eventTimestamp,
                                        Bytes.toLong(entry.getKey(), 9, Bytes.SIZEOF_LONG),
                                        rootLocationFactory.create(new URI(Bytes.toString(entry.getValue()))),
                                        logPathIdentifier.getNamespaceId(), impersonator));
            }
          }
        }

        return getFilesInRange(files, startTimestampMs);
      }
    });
  }


  @VisibleForTesting
  List<LogLocation> getFilesInRange(List<LogLocation> files, long startTimeInMs) {

    Collections.sort(files, LOG_LOCATION_COMPARATOR);
    // iterate the list from the end
    // we continue when the start timestamp of the log file is higher than the startTimeInMs
    // when we reach a file where start time is lower than the startTimeInMs we return the list from this index.
    // if we reach the beginning of the list, we return the entire list.
    List<LogLocation> filteredList = new ArrayList<>();
    long smallestTimestamp = files.get(0).getEventTimeMs();
    for (int i = files.size() - 1; i >= 0; i--) {
      LogLocation logLocation = files.get(i);
      long eventTimestamp = logLocation.getEventTimeMs();
      if (eventTimestamp < startTimeInMs) {
        if (eventTimestamp < smallestTimestamp) {
          return filteredList;
        } else {
          // assign the timestamp to smallest timestamp,
          // this allows us to add the previous files with the same event timestamp to the list before returning.
          smallestTimestamp = eventTimestamp;
        }
      }
      filteredList.add(0, logLocation);
    }
    return filteredList;
  }

  // todo : CDAP-8231 copy clean meta data logic here or separate class when implementing log cleanup task

  /**
   * Executes the given callable with a transaction. Any exception will result in {@link RuntimeException}.
   */
  private <V> V txExecute(Transactional transactional, TxCallable<V> callable) {
    try {
      return Transactions.execute(transactional, callable);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }


}
