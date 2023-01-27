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
import com.google.inject.Inject;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.util.List;
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
}
