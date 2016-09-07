/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.notifications.feeds.service;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link NotificationFeedStore} that access MDS directly.
 */
public final class MDSNotificationFeedStore implements NotificationFeedStore {

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final String TYPE_NOTIFICATION_FEED = "feed";

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;


  @Inject
  public MDSNotificationFeedStore(DatasetFramework dsFramework, TransactionSystemClient txClient) {
    this.datasetFramework = dsFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  private MetadataStoreDataset getMetadataStore(DatasetContext context) throws IOException, DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, APP_META_INSTANCE_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new MetadataStoreDataset(table);
  }

  @Override
  public Id.NotificationFeed createNotificationFeed(final Id.NotificationFeed feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<Id.NotificationFeed>() {
        @Override
        public Id.NotificationFeed call(DatasetContext context) throws Exception {
          MetadataStoreDataset metaStore = getMetadataStore(context);
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespaceId(), feed.getCategory(), feed.getName());
          Id.NotificationFeed existing = metaStore.getFirst(feedKey, Id.NotificationFeed.class);
          if (existing != null) {
            return existing;
          }
          metaStore.write(feedKey, feed);
          return null;
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public Id.NotificationFeed getNotificationFeed(final Id.NotificationFeed feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<Id.NotificationFeed>() {
        @Override
        public Id.NotificationFeed call(DatasetContext context) throws Exception {
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespaceId(), feed.getCategory(), feed.getName());
          return getMetadataStore(context).getFirst(feedKey, Id.NotificationFeed.class);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public Id.NotificationFeed deleteNotificationFeed(final Id.NotificationFeed feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<Id.NotificationFeed>() {
        @Override
        public Id.NotificationFeed call(DatasetContext context) throws Exception {
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespaceId(), feed.getCategory(), feed.getName());
          MetadataStoreDataset metaStore = getMetadataStore(context);
          Id.NotificationFeed existing = metaStore.getFirst(feedKey, Id.NotificationFeed.class);
          if (existing != null) {
            metaStore.deleteAll(feedKey);
          }
          return existing;
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public List<Id.NotificationFeed> listNotificationFeeds(final Id.Namespace namespace) {
    try {
      return Transactions.execute(transactional, new TxCallable<List<Id.NotificationFeed>>() {
        @Override
        public List<Id.NotificationFeed> call(DatasetContext context) throws Exception {
          MDSKey mdsKey = getKey(TYPE_NOTIFICATION_FEED, namespace.getId());
          return getMetadataStore(context).list(mdsKey, Id.NotificationFeed.class);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  private MDSKey getKey(String... parts) {
    return new MDSKey.Builder().add(parts).build();
  }

  private static final class NotificationFeedMds implements Iterable<MetadataStoreDataset> {
    private final MetadataStoreDataset feeds;

    private NotificationFeedMds(MetadataStoreDataset metaTable) {
      this.feeds = metaTable;
    }

    @Override
    public Iterator<MetadataStoreDataset> iterator() {
      return Iterators.singletonIterator(feeds);
    }
  }
}
