/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.metadata.lineage.DatasetField;
import io.cdap.cdap.proto.metadata.lineage.Field;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageDetails;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageSummary;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationInfo;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationInput;
import io.cdap.cdap.proto.metadata.lineage.FieldOperationOutput;
import io.cdap.cdap.proto.metadata.lineage.ProgramFieldOperationInfo;
import io.cdap.cdap.proto.metadata.lineage.ProgramInfo;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Service to compute field lineage based on operations stored in {@link DefaultFieldLineageReader}.
 */
public class FieldLineageAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(FieldLineageAdmin.class);

  private final FieldLineageReader fieldLineageReader;
  private final MetadataAdmin metadataAdmin;

  @Inject
  @VisibleForTesting
  public FieldLineageAdmin(FieldLineageReader fieldLineageReader, MetadataAdmin metadataAdmin) {
    this.fieldLineageReader = fieldLineageReader;
    this.metadataAdmin = metadataAdmin;
  }

  /**
   * Get the set of fields written to the EndPoint by field lineage {@link WriteOperation}, over the given time range,
   * optionally filtered to include only fields that have the given prefix. Additionally, can include fields from the
   * dataset schema if those field are not present in lineage information for complete and coherent information
   * about fields.
   *
   * @param endPoint the EndPoint for which the fields need to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @param prefix prefix for the field name, if {@code null} then all fields are returned
   * @param includeCurrent determines whether to include dataset's current schema fields in the response. If true the
   * result will be a union of all the fields present in lineage record with {@link Field#hasLineage} set to 'true'
   * and fields present only in dataset schema will have {@link Field#hasLineage} set to 'false'.
   *
   * @return set of fields written to a given EndPoint and any unique fields present only in the schema of the
   * dataset of includeCurrent is set to true
   */
  public Set<Field> getFields(EndPoint endPoint, long start, long end, @Nullable String prefix,
                              boolean includeCurrent) throws IOException {

    Set<String> lineageFields = fieldLineageReader.getFields(endPoint, start, end);

    Set<Field> result = createFields(lineageFields, true);
    if (includeCurrent) {
      result.addAll(createFields(getFieldsWithNoFieldLineage(endPoint, lineageFields), false));
    }
    return Strings.isNullOrEmpty(prefix) ? Collections.unmodifiableSet(result) :
      Collections.unmodifiableSet(filter(prefix, result));
  }

  /**
   * Get the summary for the specified EndPointField over a given time range depending on the direction specified.
   * Summary in the "incoming" direction consists of set of EndPointFields which participated in the computation
   * of the given EndPointField; summary in the "outgoing" direction consists of set of EndPointFields
   * which were computed from the specified EndPointField. When direction is specified as 'both', incoming as well
   * as outgoing summaries are returned.
   *
   * @param direction the direction in which summary need to be computed
   * @param endPointField the EndPointField for which summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the FieldLineageSummary
   */
  FieldLineageSummary getFieldLineage(Constants.FieldLineage.Direction direction, EndPointField endPointField,
                                      long start, long end) {
    Set<DatasetField> incoming = null;
    Set<DatasetField> outgoing = null;
    if (direction == Constants.FieldLineage.Direction.INCOMING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<EndPointField> incomingSummary = fieldLineageReader.getIncomingSummary(endPointField, start, end);
      incoming = convertSummaryToDatasetField(incomingSummary);
    }
    if (direction == Constants.FieldLineage.Direction.OUTGOING || direction == Constants.FieldLineage.Direction.BOTH) {
      Set<EndPointField> outgoingSummary = fieldLineageReader.getOutgoingSummary(endPointField, start, end);
      outgoing = convertSummaryToDatasetField(outgoingSummary);
    }
    return new FieldLineageSummary(incoming, outgoing);
  }

  /**
   * Get the summary for the specified dataset over a given time range depending on the direction specified.
   * The summary will contain all the field level lineage relations about all the fields in a dataset.
   *
   * @param direction the direction in which summary need to be computed
   * @param endPoint the EndPoint whicn represents the dataset that field level lineage needs to get computed
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the summary which contains all the field level lineage information about all the fields in a dataset
   * @throws IOException if fails to get teh schema of the dataset
   */
  public DatasetFieldLineageSummary getDatasetFieldLineage(Constants.FieldLineage.Direction direction,
                                                           EndPoint endPoint,
                                                           long start, long end) throws IOException {
    Set<String> lineageFields = fieldLineageReader.getFields(endPoint, start, end);
    Map<DatasetId, Set<FieldRelation>> incomingRelations = new HashMap<>();
    Map<DatasetId, Set<FieldRelation>> outgoingRelations = new HashMap<>();
    Map<DatasetId, Integer> fieldCount = new HashMap<>();
    for (String field : lineageFields) {
      EndPointField endPointField = new EndPointField(endPoint, field);

      // compute the incoming field level lineage
      if (direction == Constants.FieldLineage.Direction.INCOMING ||
        direction == Constants.FieldLineage.Direction.BOTH) {
        Map<DatasetId, Set<String>> incomingSummary =
          convertSummaryToDatasetMap(fieldLineageReader.getIncomingSummary(endPointField, start, end));
        // compute the field count for all incoming datasets
        incomingSummary.keySet().forEach(datasetId -> {
          fieldCount.computeIfAbsent(
            datasetId, missingDataset -> fieldLineageReader.getFields(
              EndPoint.of(missingDataset.getNamespace(), missingDataset.getDataset()), start, end).size());
        });
        // here the field itself will be the destination
        computeAndAddRelations(incomingRelations, field, true, incomingSummary);
      }

      // compute the outgoing field level lineage
      if (direction == Constants.FieldLineage.Direction.OUTGOING ||
        direction == Constants.FieldLineage.Direction.BOTH) {
        Map<DatasetId, Set<String>> outgoingSummary =
          convertSummaryToDatasetMap(fieldLineageReader.getOutgoingSummary(endPointField, start, end));
        // compute the field count for all outgoing datasets
        outgoingSummary.keySet().forEach(datasetId -> {
          fieldCount.computeIfAbsent(
            datasetId, missingDataset -> fieldLineageReader.getFields(
              EndPoint.of(missingDataset.getNamespace(), missingDataset.getDataset()), start, end).size());
        });
        // here the field itself will be the source
        computeAndAddRelations(outgoingRelations, field, false, outgoingSummary);
      }
    }

    Set<String> noLineageFields = getFieldsWithNoFieldLineage(endPoint, lineageFields);
    Set<String> allFields = ImmutableSet.<String>builder().addAll(lineageFields).addAll(noLineageFields).build();
    return new DatasetFieldLineageSummary(direction, start, end,
                                          new DatasetId(endPoint.getNamespace(), endPoint.getName()),
                                          allFields, fieldCount, incomingRelations, outgoingRelations);
  }

  /**
   * Compute the relations from the given summary and add the field relation to the map of relations. The field is
   * either the source or the destination in the relation.
   */
  private void computeAndAddRelations(Map<DatasetId, Set<FieldRelation>> relations, String field, boolean isDestination,
                                      Map<DatasetId, Set<String>> summary) {
    for (Map.Entry<DatasetId, Set<String>> entry : summary.entrySet()) {
      DatasetId outgoing = entry.getKey();
      relations.computeIfAbsent(outgoing, k -> new HashSet<>());
      entry.getValue().forEach(otherField -> relations.get(outgoing).add(
        isDestination ? new FieldRelation(otherField, field) : new FieldRelation(field, otherField)));
    }
  }

  private Set<DatasetField> convertSummaryToDatasetField(Set<EndPointField> summary) {
    Map<DatasetId, Set<String>> endPointFields = convertSummaryToDatasetMap(summary);

    Set<DatasetField> result = new HashSet<>();
    for (Map.Entry<DatasetId, Set<String>> entry : endPointFields.entrySet()) {
      result.add(new DatasetField(entry.getKey(), entry.getValue()));
    }

    return result;
  }

  private Map<DatasetId, Set<String>> convertSummaryToDatasetMap(Set<EndPointField> summary) {
    Map<DatasetId, Set<String>> endPointFields = new HashMap<>();
    for (EndPointField endPointField : summary) {
      EndPoint endPoint = endPointField.getEndPoint();
      DatasetId datasetId = new DatasetId(endPoint.getNamespace(), endPoint.getName());
      Set<String> fields = endPointFields.computeIfAbsent(datasetId, k -> new HashSet<>());
      fields.add(endPointField.getField());
    }
    return endPointFields;
  }

  /**
   * Get the operation details for the specified EndPointField over a given time range depending on the
   * direction specified. Operation details in the "incoming" direction consists of consists of the datasets
   * and their fields ({@link DatasetField}) that this field originates from, as well as the programs and
   * operations that generated this field from those origins. In outgoing direction, it consists of the datasets
   * and their fields ({@link DatasetField}) that were computed from this field, along with the programs and
   * operations that performed the computation. When direction is specified as 'both', incoming as well
   * as outgoing operations are returned.
   *
   * @param direction the direction in which operations need to be computed
   * @param endPointField the EndPointField for which operations to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the FieldLineageDetails instance
   */
  FieldLineageDetails getOperationDetails(Constants.FieldLineage.Direction direction, EndPointField endPointField,
                                          long start, long end) {
    List<ProgramFieldOperationInfo> incoming = null;
    List<ProgramFieldOperationInfo> outgoing = null;
    if (direction == Constants.FieldLineage.Direction.INCOMING || direction == Constants.FieldLineage.Direction.BOTH) {
      List<ProgramRunOperations> incomingOperations = fieldLineageReader.getIncomingOperations(endPointField, start,
                                                                                               end);
      incoming = processOperations(incomingOperations);
    }
    if (direction == Constants.FieldLineage.Direction.OUTGOING || direction == Constants.FieldLineage.Direction.BOTH) {
      List<ProgramRunOperations> outgoingOperations = fieldLineageReader.getOutgoingOperations(endPointField, start,
                                                                                               end);
      outgoing = processOperations(outgoingOperations);
    }
    return new FieldLineageDetails(incoming, outgoing);
  }

  private Set<String> getFieldsWithNoFieldLineage(EndPoint dataset,
                                                  Set<String> lineageFields) throws IOException {
    // get the system properties of this dataset
    Map<String, String> properties = metadataAdmin.getProperties(MetadataScope.SYSTEM,
                                                                 MetadataEntity.ofDataset(dataset.getNamespace(),
                                                                                          dataset.getName()));
    // the system metadata contains the schema of the dataset which is written by the DatasetSystemMetadataWriter
    if (properties.containsKey(MetadataConstants.SCHEMA_KEY)) {
      String schema = properties.get(MetadataConstants.SCHEMA_KEY);
      Schema sc = Schema.parseJson(schema);
      if (sc.getFields() != null) {
        Set<String> schemaFields = sc.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());
        // filter out the fields that are part of the lineageFields
        return sc.getFields().stream()
          .map(Schema.Field::getName)
          .filter(name -> !lineageFields.contains(name))
          .collect(Collectors.toSet());
      }
    } else {
      LOG.trace("Received request to include schema fields for {} but no schema was found. Only fields present in " +
                  "the lineage store will be returned.", dataset);
    }
    return Collections.emptySet();
  }

  private List<ProgramFieldOperationInfo> processOperations(List<ProgramRunOperations> programRunOperations) {
    List<ProgramFieldOperationInfo> result = new ArrayList<>();
    for (ProgramRunOperations entry : programRunOperations) {
      List<ProgramInfo> programInfo = computeProgramInfo(entry.getProgramRunIds());
      List<FieldOperationInfo> fieldOperationInfo = computeFieldOperationInfo(entry.getOperations());
      result.add(new ProgramFieldOperationInfo(programInfo, fieldOperationInfo));
    }
    return result;
  }

  /**
   * Computes the list of {@link ProgramInfo} from given set of ProgramRunIds.
   * For each program, there is only one item in the returned list, representing the
   * latest run of that program. Returned list is also sorted by the last executed time
   * in descending order.
   *
   * @param programRunIds set of program run ids from which program info to be computed
   * @return list of ProgramInfo
   */
  private List<ProgramInfo> computeProgramInfo(Set<ProgramRunId> programRunIds) {
    Map<ProgramId, Long> programIdToLastExecutedTime = new HashMap<>();
    for (ProgramRunId programRunId : programRunIds) {
      long programRunExecutedTime = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
      Long lastExecutedTime = programIdToLastExecutedTime.get(programRunId.getParent());
      if (lastExecutedTime == null || programRunExecutedTime > lastExecutedTime) {
        programIdToLastExecutedTime.put(programRunId.getParent(), programRunExecutedTime);
      }
    }

    Stream<Map.Entry<ProgramId, Long>> sortedByLastExecutedTime =
      programIdToLastExecutedTime.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));

    List<ProgramInfo> programInfos;

    programInfos = sortedByLastExecutedTime.map(
      programIdLongEntry -> new ProgramInfo(programIdLongEntry.getKey(),
                                            programIdLongEntry.getValue())).collect(Collectors.toList());

    return programInfos;
  }

  /**
   * Computes list of {@link FieldOperationInfo} from the given operations.
   * Returned list contains the operations sorted in topological order i.e. each operation
   * in the list is guaranteed to occur before any other operation that reads its outputs.
   *
   * @param operations set of operation to convert to FieldOperationInfo instances
   * @return list of FieldOperationInfo sorted topologically
   */
  private List<FieldOperationInfo> computeFieldOperationInfo(Set<Operation> operations) {
    List<Operation> orderedOperations = FieldLineageInfo.getTopologicallySortedOperations(operations);
    List<FieldOperationInfo> fieldOperationInfos = new ArrayList<>();
    for (Operation operation : orderedOperations) {
      fieldOperationInfos.add(convertToFieldOperationInfo(operation));
    }

    return fieldOperationInfos;
  }

  private FieldOperationInfo convertToFieldOperationInfo(Operation operation) {
    FieldOperationInput inputs = null;
    FieldOperationOutput outputs = null;
    switch (operation.getType()) {
      case READ:
        ReadOperation read = (ReadOperation) operation;
        inputs = FieldOperationInput.of(read.getSource());
        outputs = FieldOperationOutput.of(read.getOutputs());
        break;
      case TRANSFORM:
        TransformOperation transform = (TransformOperation) operation;
        inputs = FieldOperationInput.of(transform.getInputs());
        outputs = FieldOperationOutput.of(transform.getOutputs());
        break;
      case WRITE:
        WriteOperation write = (WriteOperation) operation;
        inputs = FieldOperationInput.of(write.getInputs());
        outputs = FieldOperationOutput.of(write.getDestination());
        break;
    }
    return new FieldOperationInfo(operation.getName(), operation.getDescription(), inputs, outputs);
  }

  private Set<Field> createFields(Set<String> fields, boolean hasLineage) {
    return fields.stream().map(field -> new Field(field, hasLineage)).collect(Collectors.toSet());
  }

  private Set<Field> filter(String prefix, Set<Field> fields) {
    return fields.stream().filter(field ->
                                    field.getName().toLowerCase().startsWith(prefix.toLowerCase()))
      .collect(Collectors.toSet());
  }
}
