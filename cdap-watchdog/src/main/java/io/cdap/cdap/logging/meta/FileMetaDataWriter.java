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

package io.cdap.cdap.logging.meta;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Class for writing log file meta data.
 */
public class FileMetaDataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataWriter.class);

  private final TransactionRunner transactionRunner;

  public FileMetaDataWriter(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Persists meta data associated with a log file.
   *
   * @param identifier logging context identifier.
   * @param eventTimeMs start log time associated with the file.
   * @param currentTimeMs current time during file creation.
   * @param location log file location.
   */
  public void writeMetaData(final LogPathIdentifier identifier,
                            final long eventTimeMs,
                            final long currentTimeMs,
                            final Location location) throws Exception {
    LOG.debug("Writing meta data for logging context {} with startTimeMs {} sequence Id {} and location {}",
              identifier.getRowkey(), eventTimeMs, currentTimeMs, location);
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.LogFileMetaStore.LOG_FILE_META);

      List<Field<?>> fields =
        ImmutableList.of(Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD,
                                            identifier.getRowkey()),
                         Fields.longField(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD, eventTimeMs),
                         Fields.longField(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD, currentTimeMs),
                         Fields.stringField(StoreDefinition.LogFileMetaStore.FILE_FIELD, location.toURI().getPath()));
      table.upsert(fields);
    }, IOException.class);
  }
}
