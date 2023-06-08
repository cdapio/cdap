/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.store;

import io.cdap.cdap.api.NamespaceResourceAlreadyExistsException;
import io.cdap.cdap.api.NamespaceResourceManager;
import io.cdap.cdap.api.NamespaceResourceNotFoundException;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.security.credential.Secured;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps maintain data consistency for a service using secure store in addition to structured
 * tables.
 *
 * @param <T> The metadata class for the stored entity.
 * @param <R> The entity class from which secured data may be extracted.
 */
public abstract class SecureMultiStore<T, R extends Secured<T>>
    implements NamespaceResourceManager<R> {

  private static final Logger LOG = LoggerFactory.getLogger(SecureMultiStore.class);

  final TransactionRunner transactionRunner;

  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  public SecureMultiStore(SecureStore secureStore, SecureStoreManager secureStoreManager,
      TransactionRunner transactionRunner) {
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.transactionRunner = transactionRunner;
  }

  /**
   * An identifier that uniquely identifies the class which utilizes secure store.
   */
  abstract String getResourceType();

  abstract R constructFromSecureData(T metadata, String secureData);

  abstract Optional<VersionedObject<T>> getMetadataInternal(StructuredTableContext context,
      NamespaceResourceReference ref) throws IOException;

  abstract Collection<NamespaceResourceReference> listMetadataInternal(
      StructuredTableContext context, String namespace) throws IOException;

  abstract void writeMetadataInternal(StructuredTableContext context,
      NamespaceResourceReference ref, T obj, String version) throws IOException;

  abstract void deleteMetadataInternal(StructuredTableContext context,
      NamespaceResourceReference ref) throws IOException, IllegalStateException;

  class VersionedObject<X> {
    private X x;
    private String version;

    public VersionedObject(X x, String version) {
      this.x = x;
      this.version = version;
    }

    public X getX() {
      return x;
    }

    public String getVersion() {
      return version;
    }
  }

  @Override
  public Optional<R> get(NamespaceResourceReference ref)
      throws AccessException, IOException {
    Optional<VersionedObject<T>> obj = TransactionRunners.run(transactionRunner, context -> {
      return getMetadataInternal(context, ref);
    }, IOException.class);
    if (!obj.isPresent()) {
      return Optional.empty();
    }
    String secureStoreObjKey = constructSecureStoreKey(ref, obj.get().getVersion());
    byte[] secureData;
    try {
      secureData = secureStore.getData(NamespaceId.SYSTEM.getNamespace(), secureStoreObjKey);
    } catch (Exception e) {
      throw new IOException(
          String.format("Failed to fetch data from secure store for key '%s'",
              secureStoreObjKey), e);
    }
    return Optional.of(constructFromSecureData(obj.get().getX(), new String(secureData,
        StandardCharsets.UTF_8)));
  }

  @Override
  public Collection<NamespaceResourceReference> list(String namespace)
      throws AccessException, IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return listMetadataInternal(context, namespace);
    }, IOException.class);
  }

  @Override
  public void create(NamespaceResourceReference ref, R obj)
      throws AccessException, IOException, IllegalArgumentException, IllegalStateException {
    Util.validateResourceName(ref);
    String version = createIndex(ref);
    writeSecureData(ref, obj, version);
    TransactionRunners.run(transactionRunner, context -> {
      if (getMetadataInternal(context, ref).isPresent()) {
        throw new NamespaceResourceAlreadyExistsException(ref);
      }
      writeMetadataInternal(context, ref, obj.getMetadata(), version);
      updateLiveIndex(context, ref, version);
    }, IOException.class);
  }

  @Override
  public void update(NamespaceResourceReference ref, R obj)
      throws IOException, IllegalStateException {
    String version = createIndex(ref);
    writeSecureData(ref, obj, version);
    TransactionRunners.run(transactionRunner, context -> {
      if (!getMetadataInternal(context, ref).isPresent()) {
        throw new NamespaceResourceNotFoundException(ref);
      }
      writeMetadataInternal(context, ref, obj.getMetadata(), version);
      updateLiveIndex(context, ref, version);
    }, IOException.class);
  }

  @Override
  public void delete(NamespaceResourceReference ref) throws AccessException, IOException,
      IllegalStateException, NamespaceResourceNotFoundException {
    Optional<VersionedObject<T>> obj = TransactionRunners.run(transactionRunner, context -> {
      return getMetadataInternal(context, ref);
    });
    if (!obj.isPresent()) {
      throw new NamespaceResourceNotFoundException(ref);
    }
    String secureStoreKey = constructSecureStoreKey(ref, obj.get().getVersion());
    try {
      secureStoreManager.delete(NamespaceId.SYSTEM.getNamespace(), secureStoreKey);
    } catch (Exception e) {
      // Key may have already been deleted, so just log an error.
      LOG.warn("Failed to delete key '{}' from system secure store", secureStoreKey, e);
    }
    TransactionRunners.run(transactionRunner, context -> {
      deleteMetadataInternal(context, ref);
    }, IOException.class);
  }

  /*
   * Helper methods
   */

  private String computeNamespaceSalt(String namespace) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      return Bytes.toHexString(messageDigest.digest(namespace.getBytes())).substring(0, 8);
    } catch (NoSuchAlgorithmException e) {
      // This should never happen.
      throw new RuntimeException(String.format("Failed to compute salt for namespace '%s'",
          namespace), e);
    }
  }

  private String constructSecureStoreKey(NamespaceResourceReference ref, String version) {
    return String.format("%s-%s-%s-%s-%s", getResourceType(), ref.getNamespace(),
        computeNamespaceSalt(ref.getNamespace()), ref.getName(), version);
  }

  private String createIndex(NamespaceResourceReference ref) {
    String version = UUID.randomUUID().toString();
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable secureStoreIndexTable = context
          .getTable(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_INDEX);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.INDEX_RESOURCE_TYPE,
          getResourceType()));
      fields.add(Fields.stringField(
          StoreDefinition.CredentialProvisionerStore.INDEX_RESOURCE_IDENTIFIER,
          constructSecureStoreKey(ref, version)));
      fields.add(Fields.longField(StoreDefinition.CredentialProvisionerStore.INDEX_TIMESTAMP,
          System.currentTimeMillis()));
      fields.add(Fields.booleanField(StoreDefinition.CredentialProvisionerStore.INDEX_ALIVE,
          false));
      secureStoreIndexTable.upsert(fields);
    });
    return version;
  }

  private void writeSecureData(NamespaceResourceReference ref, R obj, String version)
      throws IOException {
    String secureStoreKey = constructSecureStoreKey(ref, version);
    String secureData = "";
    if (obj.getSecureData() != null) {
      secureData = obj.getSecureData();
    }
    try {
      secureStoreManager.put(NamespaceId.SYSTEM.getNamespace(), secureStoreKey, secureData, null,
          Collections.emptyMap());
    } catch (Exception e) {
      throw new IOException(String.format("Failed to write key '%s' to system secure store",
          secureStoreKey), e);
    }
  }

  private void updateLiveIndex(StructuredTableContext context, NamespaceResourceReference ref,
      String version) throws IOException {
    StructuredTable indexTable =
        context.getTable(StoreDefinition.CredentialProvisionerStore.SECURE_STORE_INDEX);
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.CredentialProvisionerStore.INDEX_RESOURCE_TYPE,
        getResourceType()));
    fields.add(Fields.stringField(
        StoreDefinition.CredentialProvisionerStore.INDEX_RESOURCE_IDENTIFIER,
        constructSecureStoreKey(ref, version)));
    // Compare-and-swap here to ensure the index has not been deleted by the cleanup job.
    try {
      indexTable.compareAndSwap(fields,
          Fields.booleanField(StoreDefinition.CredentialProvisionerStore.INDEX_ALIVE, false),
          Fields.booleanField(StoreDefinition.CredentialProvisionerStore.INDEX_ALIVE, true));
    } catch (IllegalArgumentException e) {
      throw new ConcurrentModificationException(String.format("Failed to update live secure "
          + "store index '%s' due to concurrent modification. The index may have already been "
          + "cleaned up; please retry the request.", ref.getName()), e);
    }
  }
}
