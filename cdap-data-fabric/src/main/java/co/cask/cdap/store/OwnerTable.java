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
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import com.google.common.collect.ImmutableList;
import scala.collection.mutable.StringBuilder;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of Owner table that stores the entity and kerberos principal using StructuredTable SPI.
 */
public class OwnerTable {
  private final StructuredTable table;
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

  //Create row key from Namespaced Entity ID
  private String createRowKey(NamespacedEntityId namespacedEntityId) {
    StringBuilder builder = new StringBuilder();
    switch (namespacedEntityId.getEntityType()) {
      case NAMESPACE:
        NamespaceId id = (NamespaceId) namespacedEntityId;
        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(id.getNamespace());
        break;
      case PROGRAM:
        ProgramId program = (ProgramId) namespacedEntityId;

        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(program.getNamespace());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(APP_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(program.getApplication());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(PROGRAM_TYPE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(program.getType().name());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(PROGRAM_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(program.getProgram());
        break;
      case APPLICATION:
        ApplicationId application = (ApplicationId) namespacedEntityId;

        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(application.getNamespace());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(APP_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(application.getApplication());
        break;
      case DATASET:
        DatasetId datasetInstance = (DatasetId) namespacedEntityId;

        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(datasetInstance.getNamespace());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(DATASET_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(datasetInstance.getDataset());
        break;
      case ARTIFACT:
        ArtifactId artifactId = (ArtifactId) namespacedEntityId;

        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(artifactId.getNamespace());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(ARTIFACT_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(artifactId.getArtifact());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(ARTIFACT_VERSION_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(artifactId.getVersion());
        break;
      case SCHEDULE:
        ScheduleId scheduleId = (ScheduleId) namespacedEntityId;

        builder.append(NAMESPACE_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(scheduleId.getNamespace());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(APP_ID_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(scheduleId.getApplication());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(ARTIFACT_VERSION_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(scheduleId.getVersion());

        builder.append(ROW_KEY_SEPARATOR);
        builder.append(SCHEDULE_NAME_ROW_KEY_PREFIX);
        builder.append(ROW_KEY_SEPARATOR);
        builder.append(scheduleId.getSchedule());
        break;
      default:
        throw new IllegalArgumentException(String.format("Error converting id for entity, %s. " +
                                                           "Unexpected entity type %s",
                                                         namespacedEntityId.toString(),
                                                         namespacedEntityId.getEntityType().toString()));

    }
    return builder.toString();
  }
}
