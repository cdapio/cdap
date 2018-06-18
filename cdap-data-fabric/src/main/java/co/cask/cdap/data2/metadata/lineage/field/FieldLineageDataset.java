/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.codec.OperationTypeAdapter;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Dataset to store/retrieve field level lineage information.
 */
public class FieldLineageDataset extends AbstractDataset {

  // Storage format
  // --------------
  //
  // Checksum associated with the field lineage info:
  //                               -------------------------------------------------------
  //                               |            r          |          p                  |
  // -------------------------------------------------------------------------------------
  // | checksum | <checksum-value> | FieldLineageInfo obj  | <Presentation layer format> |
  // -------------------------------------------------------------------------------------
  //
  // Run level row:
  //                                                                           --------------------
  //                                                                           |         c        |
  // ----------------------------------------------------------------------------------------------
  // | f | <endpoint_ns>  | <endpoint_name> | <inverted-start-time> | <id.run> | <checksum-value> |
  // ----------------------------------------------------------------------------------------------
  // | b | <endpoint_ns>  | <endpoint_name> | <inverted-start-time> | <id.run> | <checksum-value> |
  // ----------------------------------------------------------------------------------------------

  public static final DatasetId FIELDLINEAGE_DATASET_ID = NamespaceId.SYSTEM.dataset("fieldLineage");

  private static final Logger LOG = LoggerFactory.getLogger(FieldLineageDataset.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  private static final byte[] RAW_FORMAT_COLS_BYTE = {'r'};
  private static final byte[] PRESENTATION_FORMAT_COLS_BYTE = {'p'};
  private static final byte[] CHECKSUM_COLS_BYTE = {'c'};

  private static final char FORWARD_LINEAGE_MARKER = 'f';
  private static final char BACKWARD_LINEAGE_MARKER = 'b';


  private final Table table;

  public FieldLineageDataset(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  /**
   * Gets an instance of {@link FieldLineageDataset}. The dataset instance will be created if it is not yet exist.
   *
   * @param datasetContext the {@link DatasetContext} for getting the dataset instance.
   * @param datasetFramework the {@link DatasetFramework} for creating the dataset instance if missing
   * @param datasetId the {@link DatasetId} of the {@link FieldLineageDataset}
   * @return an instance of {@link FieldLineageDataset}
   */
  @VisibleForTesting
  public static FieldLineageDataset getFieldLineageDataset(DatasetContext datasetContext,
                                                           DatasetFramework datasetFramework, DatasetId datasetId) {
    try {
      return DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, datasetId,
                                             FieldLineageDataset.class.getName(), DatasetProperties.EMPTY);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Store the field lineage information.
   *
   * @param info the field lineage information
   */
  public void addFieldLineageInfo(FieldLineageInfo info) {
    byte[] rowKey = getChecksumRowKey(info.getChecksum());
    table.put(rowKey, RAW_FORMAT_COLS_BYTE, Bytes.toBytes(GSON.toJson(info)));
  }

  /**
   * @return {@code true} if {@link FieldLineageInfo} exists in the store
   * corresponding to the given checksum, otherwise false is returned
   */
  public boolean hasFieldLineageInfo(long checksum) {
    return getFieldLineageInfo(checksum) != null;
  }

  /**
   * Add records referring to the common operation record having the given checksum.
   * Operations represent transformations from source endpoints to the destination endpoints.
   * From source perspective the operations are added as forward lineage, while from
   * destination perspective they are added as backward lineage.
   *
   * @param programRunId program run for which lineage is to be added
   * @param info the FieldLineageInfo created by program run
   */
  public void addFieldLineageInfoReferenceRecords(ProgramRunId programRunId, FieldLineageInfo info) {
    // For all the sources, operations represents the Forward lineage
    for (EndPoint source : info.getSources()) {
      addOperationReferenceRecord(programRunId, source, info.getChecksum(), FORWARD_LINEAGE_MARKER);
    }

    // For all the destinations, operations represents backward lineage
    for (EndPoint destination : info.getDestinations()) {
      addOperationReferenceRecord(programRunId, destination, info.getChecksum(), BACKWARD_LINEAGE_MARKER);
    }
  }

  /**
   * Get the field lineage information for a given endpoint within the given time range.
   * TODO: Read methods are mainly for the testing purpose till representation from UI is finalized.
   *
   * @param endPoint the endpoint for which lineage information to be retrieved
   * @param start start time in seconds
   * @param end end time in seconds
   * @return {@link Set} of {@link FieldLineageInfo} instances
   */
  public Set<FieldLineageInfo> getFieldLineageInfosInRange(EndPoint endPoint, long start, long end) {
    Set<Long> checksums = getChecksumsInRange(getScanStartKey(endPoint, end, BACKWARD_LINEAGE_MARKER),
                                              getScanEndKey(endPoint, start, BACKWARD_LINEAGE_MARKER));

    Set<FieldLineageInfo> infos = new HashSet<>();
    for (long checksum : checksums) {
      infos.add(getFieldLineageInfo(checksum));
    }
    return infos;
  }

  private FieldLineageInfo getFieldLineageInfo(long checksum) {
    byte[] rowKey = getChecksumRowKey(checksum);
    byte[] bytes = table.get(rowKey, RAW_FORMAT_COLS_BYTE);
    return GSON.fromJson(Bytes.toString(bytes), FieldLineageInfo.class);
  }

  private byte[] getScanStartKey(EndPoint endPoint, long end, char lineageDirectionMarker) {
    // time is inverted, hence we need to have end time in start key.
    return getScanKey(endPoint, end, lineageDirectionMarker);
  }

  private byte[] getScanEndKey(EndPoint endPoint, long start, char lineageDirectionMarker) {
    // time is inverted, hence we need to have start time in end key.
    return getScanKey(endPoint, start, lineageDirectionMarker);
  }

  private Set<Long> getChecksumsInRange(byte[] startKey, byte[] endKey) {
    Set<Long> checksums = new HashSet<>();
    try (Scanner scanner = table.scan(startKey, endKey)) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }

        checksums.add(Bytes.toLong(row.get(CHECKSUM_COLS_BYTE)));
      }
    }
    return checksums;
  }

  private byte[] getScanKey(EndPoint endPoint, long time, char lineageDirectionMarker) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(lineageDirectionMarker);
    addEndPoint(builder, endPoint);
    builder.add(invertTime(time));
    return builder.build().getKey();
  }

  private void addOperationReferenceRecord(ProgramRunId programRunId, EndPoint endPoint, long checksum,
                                           char lineageDirectionMarker) {
    byte[] bytes = getProgramRunKey(lineageDirectionMarker, programRunId, endPoint);
    table.put(bytes, CHECKSUM_COLS_BYTE, Bytes.toBytes(checksum));
  }

  private byte[] getChecksumRowKey(long checksum) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add("c");
    builder.add(checksum);
    return builder.build().getKey();
  }

  private byte[] getProgramRunKey(char lineageDirectionMarker, ProgramRunId programRunId, EndPoint endPoint) {
    long invertedStartTime = getInvertedStartTime(programRunId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(lineageDirectionMarker);
    addEndPoint(builder, endPoint);
    builder.add(invertedStartTime);
    addProgram(builder, programRunId.getParent());
    builder.add(programRunId.getEntityName());
    return builder.build().getKey();
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(ProgramRunId run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getEntityName()), TimeUnit.MILLISECONDS));
  }

  private void addEndPoint(MDSKey.Builder keyBuilder, EndPoint endPoint) {
    keyBuilder.add(endPoint.getNamespace())
      .add(endPoint.getName());
  }

  private void addProgram(MDSKey.Builder keyBuilder, ProgramId program) {
    keyBuilder.add(program.getNamespace())
      .add(program.getParent().getEntityName())
      .add(program.getType().getCategoryName())
      .add(program.getEntityName());
  }
}
