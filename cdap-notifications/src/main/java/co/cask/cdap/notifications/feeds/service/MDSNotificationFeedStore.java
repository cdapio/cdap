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
import co.cask.cdap.notifications.NotificationFeedInfoDeserializer;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of {@link NotificationFeedStore} that access MDS directly.
 */
public final class MDSNotificationFeedStore implements NotificationFeedStore {

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final String TYPE_NOTIFICATION_FEED = "feed";
  private static final Gson GSON = new GsonBuilder()
    .enableComplexMapKeySerialization()
    .registerTypeAdapter(NotificationFeedInfo.class, new NotificationFeedInfoDeserializer())
    .create();

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
    return new MetadataStoreDataset(table, GSON);
  }

  @Override
  public NotificationFeedInfo createNotificationFeed(final NotificationFeedInfo feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<NotificationFeedInfo>() {
        @Override
        public NotificationFeedInfo call(DatasetContext context) throws Exception {
          MetadataStoreDataset metaStore = getMetadataStore(context);
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespace(), feed.getCategory(), feed.getFeed());
          NotificationFeedInfo existing = metaStore.getFirst(feedKey, NotificationFeedInfo.class);
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
  public NotificationFeedInfo getNotificationFeed(final NotificationFeedId feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<NotificationFeedInfo>() {
        @Override
        public NotificationFeedInfo call(DatasetContext context) throws Exception {
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespace(), feed.getCategory(), feed.getFeed());
          return  getMetadataStore(context).getFirst(feedKey, NotificationFeedInfo.class);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public NotificationFeedInfo deleteNotificationFeed(final NotificationFeedId feed) {
    try {
      return Transactions.execute(transactional, new TxCallable<NotificationFeedInfo>() {
        @Override
        public NotificationFeedInfo call(DatasetContext context) throws Exception {
          MDSKey feedKey = getKey(TYPE_NOTIFICATION_FEED, feed.getNamespace(), feed.getCategory(), feed.getFeed());
          MetadataStoreDataset metaStore = getMetadataStore(context);
          NotificationFeedInfo existing = metaStore.getFirst(feedKey, NotificationFeedInfo.class);
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
  public List<NotificationFeedInfo> listNotificationFeeds(final NamespaceId namespace) {
    try {
      return Transactions.execute(transactional, new TxCallable<List<NotificationFeedInfo>>() {
        @Override
        public List<NotificationFeedInfo> call(DatasetContext context) throws Exception {
          MDSKey mdsKey = getKey(TYPE_NOTIFICATION_FEED, namespace.getNamespace());
          return getMetadataStore(context).list(mdsKey, NotificationFeedInfo.class);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  private MDSKey getKey(String... parts) {
    return new MDSKey.Builder().add(parts).build();
  }

}
