/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link NamespaceStore}.
 */
public class DefaultNamespaceStore implements NamespaceStore {

  private final TransactionRunner transactionRunner;

  @Inject
  public DefaultNamespaceStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  private NamespaceTable getNamespaceTable(StructuredTableContext context) throws TableNotFoundException {
    return new NamespaceTable(context);
  }

  @Override
  @Nullable
  public NamespaceMeta create(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    return TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable mds = getNamespaceTable(context);
      NamespaceMeta existing = mds.get(metadata.getNamespaceId());
      if (existing != null) {
        return existing;
      }
      mds.create(metadata);
      return null;
    });
  }

  @Override
  public void update(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable mds = getNamespaceTable(context);
      NamespaceMeta existing = mds.get(metadata.getNamespaceId());
      if (existing != null) {
        mds.create(metadata);
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta get(final NamespaceId id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return TransactionRunners.run(transactionRunner, context -> {
      return getNamespaceTable(context).get(id);
    });
  }

  @Override
  @Nullable
  public NamespaceMeta delete(final NamespaceId id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable mds = getNamespaceTable(context);
      NamespaceMeta existing = mds.get(id);
      if (existing != null) {
        mds.delete(id);
      }
      return existing;
    });
  }

  @Override
  public List<NamespaceMeta> list() {
    return TransactionRunners.run(transactionRunner, context -> {
      return getNamespaceTable(context).list();
    });
  }

  @Override
  public long getNamespaceCount() {
    return TransactionRunners.run(transactionRunner, context -> {
      return getNamespaceTable(context).getNamespaceCount();
    });
  }

  @Override
  public void updateProperties(NamespaceId namespaceId, NamespaceMeta namespaceMeta) throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable table = getNamespaceTable(context);
      updatePropertiesInternal(table, namespaceId, namespaceMeta);
    }, BadRequestException.class);
  }

  private void updatePropertiesInternal(NamespaceTable table, NamespaceId namespaceId, NamespaceMeta namespaceMeta)
    throws Exception {
    NamespaceMeta existingMeta = table.get(namespaceId);
    if (existingMeta == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(existingMeta);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    NamespaceConfig config = namespaceMeta.getConfig();
    if (config != null && !Strings.isNullOrEmpty(config.getSchedulerQueueName())) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    // TODO: refactor this class, move out the TransactionRunner
    if (config != null && config.getKeytabURI() != null) {
      String keytabURI = config.getKeytabURI();
      if (keytabURI.isEmpty()) {
        throw new BadRequestException("Cannot update keytab URI with an empty URI.");
      }
      String existingKeytabURI = existingMeta.getConfig().getKeytabURIWithoutVersion();
      if (existingKeytabURI == null) {
        throw new BadRequestException("Cannot update keytab URI since there is no existing principal or keytab URI.");
      }
      if (keytabURI.equals(existingKeytabURI)) {
        // The given keytab URI is the same as the existing one, but the content of the keytab file might be changed.
        // Increment the keytab URI version so that the cache will reload content in the updated keytab file.
        builder.incrementKeytabURIVersion();
      } else {
        builder.setKeytabURIWithoutVersion(keytabURI);
        // clear keytab URI version
        builder.setKeytabURIVersion(0);
      }
    }

    Set<String> difference = existingMeta.getConfig().getDifference(config);
    if (!difference.isEmpty()) {
      throw new BadRequestException(String.format("Mappings %s for namespace %s cannot be updated " +
                                                    "once the namespace is created.", difference, namespaceId));
    }
    table.create(builder.build());
  }

  @Override
  public void setRepository(NamespaceId namespaceId, RepositoryConfig repository) throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable table = getNamespaceTable(context);

      NamespaceMeta existingMeta = table.get(namespaceId);
      if (existingMeta == null) {
        throw new NamespaceNotFoundException(namespaceId);
      }
      
      NamespaceMeta.Builder builder = new NamespaceMeta.Builder(existingMeta).setRepository(repository);
      NamespaceMeta updatedMeta = builder.build();
      table.create(updatedMeta);
    }, NamespaceNotFoundException.class);
  }

  @Override
  public void deleteRepository(NamespaceId namespaceId) throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable table = getNamespaceTable(context);

      NamespaceMeta existingMeta = table.get(namespaceId);
      if (existingMeta == null) {
        throw new NamespaceNotFoundException(namespaceId);
      }
      if (existingMeta.getRepository() == null) {
        throw new RepositoryNotFoundException(namespaceId);
      }

      NamespaceMeta.Builder builder = new NamespaceMeta.Builder(existingMeta).setRepository(null);
      NamespaceMeta updatedMeta = builder.build();
      table.create(updatedMeta);
    }, NamespaceNotFoundException.class, RepositoryNotFoundException.class);
  }
}
