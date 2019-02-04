/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import scala.collection.mutable.StringBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of Owner table that stores the entity and kerberos principal using StructuredTable SPI.
 */
public class OwnerTable {
  private final StructuredTable table;
  private static final Map<Class<? extends NamespacedEntityId>, String> TYPE_MAP =
    ImmutableMap.<Class<? extends NamespacedEntityId>, String>builder()
      .put(NamespaceId.class, EntityTypeSimpleName.NAMESPACE.getSerializedForm())
      .put(ArtifactId.class, EntityTypeSimpleName.ARTIFACT.getSerializedForm())
      .put(ApplicationId.class, EntityTypeSimpleName.APP.getSerializedForm())
      .put(ProgramId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(WorkflowId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(ServiceId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(DatasetId.class, EntityTypeSimpleName.DATASET.getSerializedForm())
      .put(ScheduleId.class, EntityTypeSimpleName.SCHEDULE.getSerializedForm())
      .build();

  private static final String ROW_KEY_SEPARATOR = ":";
  private static final String NAMESPACE_ROW_KEY_PREFIX = "n";
  private static final String APP_ID_ROW_KEY_PREFIX = "ap";
  private static final String PROGRAM_ID_ROW_KEY_PREFIX = "p";
  private static final String PROGRAM_TYPE_ROW_KEY_PREFIX = "t";
  private static final String DATASET_ID_ROW_KEY_PREFIX = "d";
  private static final String ARTIFACT_ID_ROW_KEY_PREFIX = "at";
  private static final String ARTIFACT_VERSION_ROW_KEY_PREFIX = "av";
  private static final String SCHEDULE_NAME_ROW_KEY_PREFIX = "av";

  public OwnerTable(StructuredTableContext context) throws TableNotFoundException {
    this.table = context.getTable(StoreDefinition.OwnerStore.OWNER_TABLE);
  }

  /**
   * Add the kerberos principal that is keyed by entityId.
   *
   * @param entityId NamespacedEntityId that is stored.
   * @param kerberosPrincipalId KerberosPrincipalId that is stored.
   * @throws IOException is thrown when there is an error writing to the table.
   * @throws AlreadyExistsException is thrown when the key entityId already exists.
   */
  public void add(final NamespacedEntityId entityId,
                  final KerberosPrincipalId kerberosPrincipalId) throws IOException, AlreadyExistsException {
    Optional<StructuredRow> row = table.read(ImmutableList.of
        (Fields.stringField(StoreDefinition.OwnerStore.PRINCIPAL_FIELD, createRowKey(entityId))));
    if (row.isPresent()) {
      throw new AlreadyExistsException(entityId,
          String.format("Owner information already exists for entity '%s'.",
              entityId));
    }
    Field<String> principalField = Fields.stringField(StoreDefinition.OwnerStore.PRINCIPAL_FIELD,
                                                      createRowKey(entityId));
    Field<byte[]> keytabField = Fields.bytesField(StoreDefinition.OwnerStore.KEYTAB_FIELD,
        Bytes.toBytes(kerberosPrincipalId.getPrincipal()));
    table.upsert(ImmutableList.of(principalField, keytabField));
  }

  /**
   * Function that checks if a given entity id exists.
   *
   * @param entityId NamespacedEntityId to check for existence.
   * @return true if the entityId exists, false otherwise.
   * @throws IOException is thrown when there is an error reading from the table.
   */
  public boolean exists(final NamespacedEntityId entityId) throws IOException {
    Optional<StructuredRow> row = table.read(ImmutableList.of
        (Fields.stringField(StoreDefinition.OwnerStore.PRINCIPAL_FIELD, createRowKey(entityId))));
    return row.isPresent();
  }

  /**
   * Get Kerberos principal for a given entity id
   *
   * @param entityId NamespacedEntityId for the requested KerberosPrincipalId.
   * @return KerberosPrincipalId for the given entityId.
   * @throws IOException is thrown when there is an error reading from the table.
   */
  @Nullable
  public KerberosPrincipalId getOwner(final NamespacedEntityId entityId) throws IOException {
    Optional<StructuredRow> row = table.read(ImmutableList.of
        (Fields.stringField(StoreDefinition.OwnerStore.PRINCIPAL_FIELD, createRowKey(entityId))));

    return row.isPresent() ?
        new KerberosPrincipalId(Bytes.toString(row.get().getBytes(StoreDefinition.OwnerStore.KEYTAB_FIELD))) : null;
  }

  /**
   * Delete the entityId and corresponding kerberos principalId.
   *
   * @param entityId NamespacedEntityId to delete.
   * @throws IOException is thrown when there is an error deleting the entry.
   */
  public void delete(final NamespacedEntityId entityId) throws IOException {
    table.delete(ImmutableList.of(Fields.stringField(StoreDefinition.OwnerStore.PRINCIPAL_FIELD,
                                                     createRowKey(entityId))));
  }

  private String createRowKey(NamespacedEntityId namespacedEntityId) {
    StringBuilder builder = new StringBuilder();
    String type = TYPE_MAP.get(namespacedEntityId.getClass());
    if (type.equals(TYPE_MAP.get(NamespaceId.class))) {
      NamespaceId namespaceId = (NamespaceId) namespacedEntityId;
      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId.getNamespace());
    } else if (type.equals(TYPE_MAP.get(ProgramId.class))) {
      ProgramId program = (ProgramId) namespacedEntityId;
      String namespaceId = program.getNamespace();
      String appId = program.getApplication();
      String programType = program.getType().name();
      String programId = program.getProgram();

      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(APP_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(appId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(PROGRAM_TYPE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(programType);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(PROGRAM_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(programId);
    } else if (type.equals(TYPE_MAP.get(ApplicationId.class))) {
      ApplicationId application = (ApplicationId) namespacedEntityId;
      String namespaceId = application.getNamespace();
      String appId = application.getApplication();

      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(APP_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(appId);
    } else if (type.equals(TYPE_MAP.get(DatasetId.class))) {
      DatasetId datasetInstance = (DatasetId) namespacedEntityId;
      String namespaceId = datasetInstance.getNamespace();
      String datasetId = datasetInstance.getDataset();

      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(DATASET_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(datasetId);
    } else if (type.equals(TYPE_MAP.get(ArtifactId.class))) {
      ArtifactId artifactId = (ArtifactId) namespacedEntityId;
      String namespaceId = artifactId.getNamespace();
      String name = artifactId.getArtifact();
      String version = artifactId.getVersion();

      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(ARTIFACT_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(name);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(ARTIFACT_VERSION_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(version);
    } else if (type.equals(TYPE_MAP.get(ScheduleId.class))) {
      ScheduleId scheduleId = (ScheduleId) namespacedEntityId;
      String namespaceId = scheduleId.getNamespace();
      String appId = scheduleId.getApplication();
      String version = scheduleId.getVersion();
      String scheduleName = scheduleId.getSchedule();

      builder.append(NAMESPACE_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(namespaceId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(APP_ID_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(appId);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(ARTIFACT_VERSION_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(version);

      builder.append(ROW_KEY_SEPARATOR);
      builder.append(SCHEDULE_NAME_ROW_KEY_PREFIX);
      builder.append(ROW_KEY_SEPARATOR);
      builder.append(scheduleName);
    } else {
      throw new IllegalArgumentException("Unexpected .");
    }
    return builder.toString();
  }
}
