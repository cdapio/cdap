/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.messaging.store.cache;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.store.ForwardingTableFactory;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TableFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;

/**
 * A {@link TableFactory} with optional caching for {@link MessageTable} that it creates.
 */
public class CachingTableFactory extends ForwardingTableFactory {

  public static final String DELEGATE_TABLE_FACTORY = "delegate.table.factory";

  private final CConfiguration cConf;
  private final TableFactory delegateTableFactory;
  private final MessageTableCacheProvider cacheProvider;

  @Inject
  CachingTableFactory(CConfiguration cConf,
                      @Named(DELEGATE_TABLE_FACTORY) TableFactory delegateTableFactory,
                      MessageTableCacheProvider cacheProvider) {
    this.cConf = cConf;
    this.delegateTableFactory = delegateTableFactory;
    this.cacheProvider = cacheProvider;
  }

  @Override
  public TableFactory getDelegate() {
    return delegateTableFactory;
  }

  @Override
  public MetadataTable createMetadataTable(String tableName) throws IOException {
    return delegateTableFactory.createMetadataTable(tableName);
  }

  @Override
  public MessageTable createMessageTable(String tableName) throws IOException {
    MessageTable messageTable = delegateTableFactory.createMessageTable(tableName);
    return new CachingMessageTable(cConf, messageTable, cacheProvider);
  }

  @Override
  public PayloadTable createPayloadTable(String tableName) throws IOException {
    return delegateTableFactory.createPayloadTable(tableName);
  }
}
