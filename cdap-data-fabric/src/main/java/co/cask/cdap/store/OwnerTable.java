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
import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation of Owner table that stores the entity and kerberos principal using StructuredTable SPI.
 */
public class OwnerTable {
  private final StructuredTable table;

  public OwnerTable(StructuredTableContext context) throws TableNotFoundException {
    this.table = context.getTable(StoreDefinition.OwnerStore.OWNER_MDS_TABLE);
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

  private static String createRowKey(NamespacedEntityId targetId) {
    // We are not going to upgrade owner.meta table in 5.0, when we upgrade this table,
    // we should call  EntityIdKeyHelper#getTargetType()
    String targetType = EntityIdKeyHelper.getV1TargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(targetType);
    EntityIdKeyHelper.addTargetIdToKey(builder, targetId);
    MDSKey build = builder.build();
    return build.toString();
  }
}
