/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.datapipeline.service.StudioUtil;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Schema for draft store.
 */
public class DraftStore {
  public static final StructuredTableId TABLE_ID = new StructuredTableId("drafts");
  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String OWNER_COL = "owner";
  private static final String ID_COL = "id";
  private static final String ARTIFACT_COL = "artifact";
  private static final String NAME_COL = "name";
  private static final String DESCRIPTION_COL = "description";
  private static final String CREATED_COL = "createdTimeMillis";
  private static final String UPDATED_COL = "updatedTimeMillis";
  private static final String PIPELINE_COL = "pipeline";
  private static final String REVISION_COL = "revision";
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(Fields.stringType(NAMESPACE_COL),
                Fields.longType(GENERATION_COL),
                Fields.stringType(OWNER_COL),
                Fields.stringType(ID_COL),
                Fields.stringType(ARTIFACT_COL),
                Fields.stringType(NAME_COL),
                Fields.stringType(DESCRIPTION_COL),
                Fields.longType(CREATED_COL),
                Fields.longType(UPDATED_COL),
                Fields.stringType(PIPELINE_COL),
                Fields.intType(REVISION_COL))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, OWNER_COL, ID_COL)
    .build();
  private static final Gson GSON = new Gson();
  private final TransactionRunner transactionRunner;

  public DraftStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * @param namespace the namespace to fetch the drafts from
   * @param owner the id of the owner of the drafts
   * @param sortRequest The sorting that should be applied to the results, pass null if no sorting is required.
   * @return a list of drafts
   * @throws TableNotFoundException if the draft store table is not found
   * @throws InvalidFieldException if the fields Namespace and owner fields do not match the fields in the
   *   StructuredTable
   */
  public List<Draft> listDrafts(NamespaceSummary namespace, String owner,
                                SortRequest sortRequest,
                                boolean includeConfig) throws TableNotFoundException, InvalidFieldException {
    List<Field<?>> prefix = new ArrayList<>(3);
    prefix.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    prefix.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    prefix.add(Fields.stringField(OWNER_COL, owner));

    List<StructuredRow> rows;
    rows = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Range range = Range.singleton(prefix);
      List<StructuredRow> temp = new ArrayList<>();
      try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
        rowIter.forEachRemaining(temp::add);
      }
      return temp;
    }, TableNotFoundException.class, InvalidFieldException.class);

    List<StructuredRow> sortedResults = doSort(rows, sortRequest);
    return sortedResults.stream().map(row -> fromRow(row, includeConfig)).collect(Collectors.toList());
  }

  /**
   * Helper method to apply the sorting onto a list of rows. Sorting needs to take place at the store-level so it can
   * leverage the StructuredRow to enable sorting on any field.
   *
   * @param rows list of {@link StructuredRow} to be sorted
   * @param sortRequest {@link SortRequest} describing the sort to be performed
   * @return a sorted list of {@link StructuredRow}
   */
  private List<StructuredRow> doSort(List<StructuredRow> rows, @Nullable SortRequest sortRequest) {
    if (sortRequest == null) {
      return rows;
    }
    String sortField = sortRequest.getFieldName();
    FieldType field = TABLE_SPEC.getFieldTypes().stream()
      .filter(f -> f.getName().equals(sortField))
      .findFirst()
      .orElse(null);
    if (field == null) {
      throw new IllegalArgumentException(
        String
          .format("Invalid value '%s' for sortBy. This field does not exist in the Drafts table.",
                  sortField));
    }

    FieldType.Type fieldType = field.getType();
    Comparator<StructuredRow> comparator;
    switch (fieldType) {
      case STRING:
        comparator = Comparator.<StructuredRow, String>comparing(o -> o.getString(sortField));
        break;
      case INTEGER:
        comparator = Comparator.<StructuredRow, Integer>comparing(o -> o.getInteger(sortField));
        break;
      case LONG:
        comparator = Comparator.<StructuredRow, Long>comparing(o -> o.getLong(sortField));
        break;
      case FLOAT:
        comparator = Comparator.<StructuredRow, Float>comparing(o -> o.getFloat(sortField));
        break;
      case DOUBLE:
        comparator = Comparator.<StructuredRow, Double>comparing(o -> o.getDouble(sortField));
        break;
      case BYTES:
        comparator = Comparator.comparing(o -> o.getBytes(sortField), Bytes.BYTES_COMPARATOR);
        break;
      default:
        throw new NotImplementedException(String.format("Cannot sort field '%s' because type '%s' is not supported.",
                                                        sortField, fieldType.toString()));
    }

    if (sortRequest.getOrder() != SortRequest.SortOrder.ASC) {
      comparator = comparator.reversed();
    }

    rows.sort(comparator);

    return rows;
  }

  /**
   * Fetch a given draft if it exists
   *
   * @param id {@link DraftId} that is used to uniquely identify a draft
   * @return an {@link Optional<Draft>} representing the requested draft
   * @throws TableNotFoundException if the draft store table is not found
   * @throws InvalidFieldException if the fields in the {@link DraftId} object do not match the fields in the
   *   StructuredTable
   */
  public Optional<Draft> getDraft(DraftId id) throws TableNotFoundException, InvalidFieldException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Optional<StructuredRow> row = table.read(getKey(id));
      return row.map(this::fromRow);
    }, TableNotFoundException.class, InvalidFieldException.class);
  }

  /**
   * Delete the given draft. This is a no-op if the draft does not exist
   *
   * @param id {@link DraftId} that is used to uniquely identify a draft
   * @throws TableNotFoundException if the draft store table is not found
   * @throws InvalidFieldException if the fields in the {@link Draft} object do not match the fields in the
   *   StructuredTable
   */
  public void deleteDraft(DraftId id) throws TableNotFoundException, InvalidFieldException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.delete(getKey(id));
    }, TableNotFoundException.class, InvalidFieldException.class);
  }

  /**
   * Create/update the given draft
   *
   * @param id {@link DraftId} that is used to uniquely identify a draft
   * @param request {@link DraftStoreRequest} that contains the rest of the draft data
   * @throws TableNotFoundException if the draft store table is not found
   * @throws InvalidFieldException if the fields in the {@link Draft} or {@link DraftStoreRequest} objects do not
   *   match the fields in the StructuredTable
   */
  public <T extends ETLConfig> void writeDraft(DraftId id, DraftStoreRequest<T> request)
    throws TableNotFoundException, InvalidFieldException {

    Optional<Draft> existing = getDraft(id);
    long now = System.currentTimeMillis();
    long createTime = existing.map(Draft::getCreatedTimeMillis).orElse(now);

    Draft draft = new Draft(request.getConfig(), request.getName(), request.getDescription(), request.getArtifact(),
                            id.getId(), createTime, now);

    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.upsert(getRow(id, draft));
    }, TableNotFoundException.class, InvalidFieldException.class);
  }

  /**
   * Returns the count of drafts in the table
   *
   * @return long value presenting the number of drafts in the table
   * @throws TableNotFoundException if the draft store table is not found
   */
  public long getDraftCount() throws TableNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      return table.count(Collections.singleton(Range.all()));
    }, TableNotFoundException.class);
  }

  private void addKeyFields(DraftId id, List<Field<?>> fields) {
    fields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    fields.add(Fields.stringField(OWNER_COL, id.getOwner()));
    fields.add(Fields.stringField(ID_COL, id.getId()));
  }

  private List<Field<?>> getKey(DraftId id) {
    List<Field<?>> keyFields = new ArrayList<>(4);
    addKeyFields(id, keyFields);
    return keyFields;
  }

  private List<Field<?>> getRow(DraftId id, Draft draft) {
    List<Field<?>> fields = new ArrayList<>(11);
    addKeyFields(id, fields);
    fields.add(Fields.stringField(ARTIFACT_COL, GSON.toJson(draft.getArtifact())));
    fields.add(Fields.stringField(NAME_COL, draft.getName()));
    fields.add(Fields.stringField(DESCRIPTION_COL, draft.getDescription()));
    fields.add(Fields.longField(CREATED_COL, draft.getCreatedTimeMillis()));
    fields.add(Fields.longField(UPDATED_COL, draft.getUpdatedTimeMillis()));
    fields.add(Fields.stringField(PIPELINE_COL, GSON.toJson(draft.getConfig())));
    fields.add(Fields.intField(REVISION_COL, draft.getRevision()));

    return fields;
  }

  private Draft fromRow(StructuredRow row) {
    return fromRow(row, true);
  }

  @SuppressWarnings("ConstantConditions")
  private Draft fromRow(StructuredRow row, boolean includeConfig) {
    String id = row.getString(ID_COL);
    String name = row.getString(NAME_COL);
    String description = row.getString(DESCRIPTION_COL);
    long createTime = row.getLong(CREATED_COL);
    long updateTime = row.getLong(UPDATED_COL);

    String artifactStr = row.getString(ARTIFACT_COL);
    ArtifactSummary artifact = GSON.fromJson(artifactStr, ArtifactSummary.class);
    String configStr = row.getString(PIPELINE_COL);
    ETLConfig config = null;
    if (includeConfig) {
      if (StudioUtil.isBatchPipeline(artifact)) {
        config = GSON.fromJson(configStr, ETLBatchConfig.class);
      } else if (StudioUtil.isStreamingPipeline(artifact)) {
        config = GSON.fromJson(configStr, DataStreamsConfig.class);
      } else {
        throw new
          IllegalArgumentException(
          String.format("Failed to parse pipeline config string: %s is not a supported pipeline type",
                        artifact.getName()));
      }
    }

    return new Draft(config, name, description, artifact, id, createTime, updateTime);
  }
}
