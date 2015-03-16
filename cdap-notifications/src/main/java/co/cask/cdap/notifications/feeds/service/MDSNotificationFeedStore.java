/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link NotificationFeedStore} that access MDS directly.
 */
public final class MDSNotificationFeedStore implements NotificationFeedStore {
  private static final Logger LOG = LoggerFactory.getLogger(MDSNotificationFeedStore.class);

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final String NOTIFICATION_FEED_TABLE = "app.meta";
  private static final String TYPE_NOTIFICATION_FEED = "feed";

  private Transactional<NotificationFeedMds, MetadataStoreDataset> txnl;

  @Inject
  public MDSNotificationFeedStore(TransactionExecutorFactory txExecutorFactory, final DatasetFramework dsFramework) {

    txnl = Transactional.of(txExecutorFactory, new Supplier<NotificationFeedMds>() {
      @Override
      public NotificationFeedMds get() {
        try {
          Id.DatasetInstance notificationsDatasetInstanceId = Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE,
                                                                                      NOTIFICATION_FEED_TABLE);
          Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, notificationsDatasetInstanceId, "table",
                                                           DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS,
                                                           null);

          return new NotificationFeedMds(new MetadataStoreDataset(mdsTable));
        } catch (Exception e) {
          LOG.debug("Failed to access app.meta table", e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public Id.NotificationFeed createNotificationFeed(final Id.NotificationFeed feed) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NotificationFeedMds, Id.NotificationFeed>() {
      @Override
      public Id.NotificationFeed apply(NotificationFeedMds input) throws Exception {
        String feedId = feed.getId();
        Id.NotificationFeed existing = input.feeds.get(getKey(feedId), Id.NotificationFeed.class);
        if (existing != null) {
          return existing;
        }
        input.feeds.write(getKey(feed.getId()), feed);
        return null;
      }
    });
  }

  @Override
  public Id.NotificationFeed getNotificationFeed(final String feedId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NotificationFeedMds, Id.NotificationFeed>() {
      @Override
      public Id.NotificationFeed apply(NotificationFeedMds input) throws Exception {
        return input.feeds.get(getKey(feedId), Id.NotificationFeed.class);
      }
    });
  }

  @Override
  public Id.NotificationFeed deleteNotificationFeed(final String feedId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NotificationFeedMds, Id.NotificationFeed>() {
      @Override
      public Id.NotificationFeed apply(NotificationFeedMds input) throws Exception {
        Id.NotificationFeed existing = input.feeds.get(getKey(feedId), Id.NotificationFeed.class);
        if (existing != null) {
          input.feeds.deleteAll(getKey(feedId));
        }
        return existing;
      }
    });
  }

  @Override
  public List<Id.NotificationFeed> listNotificationFeeds(final Id.Namespace namespace) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NotificationFeedMds, List<Id.NotificationFeed>>() {
      @Override
      public List<Id.NotificationFeed> apply(NotificationFeedMds input) throws Exception {
        return input.feeds.list(getKey(namespace.getId()), Id.NotificationFeed.class);
      }
    });
  }

  private MDSKey getKey(String id) {
    return new MDSKey.Builder().add(TYPE_NOTIFICATION_FEED, id).build();
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
