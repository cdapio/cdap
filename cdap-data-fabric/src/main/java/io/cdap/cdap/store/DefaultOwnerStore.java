/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.store;

import com.google.inject.Inject;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class manages owner's principal information of CDAP entities.
 * <p>
 * Currently: Owner information is stored for the following entities:
 * <ul>
 * <li>{@link io.cdap.cdap.api.dataset.Dataset}</li>
 * <li>{@link io.cdap.cdap.api.app.Application}</li>
 * <li>{@link io.cdap.cdap.common.conf.Constants.Namespace}</li>
 * <p>
 * </ul>
 * </p>
 * <p>
 * It is the responsibility of the creator of the supported entities to add an entry in this store to store the
 * associated owner's principal. Note: An absence of an entry in this table for an {@link EntityId} does not
 * signifies that the entity does not exists. The owner information is only stored if an owner was provided during
 * creation time else the owner information is non-existent which signifies that the entity own is default CDAP owner.
 * </p>
 */
public class DefaultOwnerStore extends OwnerStore {


  private final TransactionRunner txRunner;

  @Inject
  DefaultOwnerStore(TransactionRunner txRunner) {
    this.txRunner = txRunner;
  }

  private OwnerTable getOwnerTable(StructuredTableContext context) throws TableNotFoundException {
    return new OwnerTable(context);
  }

  @Override
  public void add(final NamespacedEntityId entityId,
                  final KerberosPrincipalId kerberosPrincipalId) throws IOException, AlreadyExistsException {
    validate(entityId, kerberosPrincipalId);
    TransactionRunners.run(txRunner, context -> {
      OwnerTable ownerTable = getOwnerTable(context);
      ownerTable.add(entityId, kerberosPrincipalId);
    }, AlreadyExistsException.class, IOException.class);
  }

  @Override
  @Nullable
  public KerberosPrincipalId getOwner(final NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    return TransactionRunners.run(txRunner, context -> {
      OwnerTable ownerTable = getOwnerTable(context);
      return ownerTable.getOwner(entityId);
    }, IOException.class);
  }

  @Override
  public <T extends NamespacedEntityId> Map<T, KerberosPrincipalId> getOwners(Set<T> ids) throws IOException {
    ids.forEach(this::validate);
    return TransactionRunners.run(txRunner, context -> {
      return getOwnerTable(context).getOwners(ids);
    }, IOException.class);
  }

  @Override
  public boolean exists(final NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    return TransactionRunners.run(txRunner, context -> {
      OwnerTable ownerTable = getOwnerTable(context);
      return ownerTable.exists(entityId);
    }, IOException.class);
  }

  @Override
  public void delete(final NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    TransactionRunners.run(txRunner, context -> {
      OwnerTable ownerTable = getOwnerTable(context);
      ownerTable.delete(entityId);
    }, IOException.class);
  }


}
