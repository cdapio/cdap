/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.postgres;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.store.MessageTable;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.sql.DataSource;

public class PostgresMessageTable implements MessageTable {
  private final DataSource dataSource;

  PostgresMessageTable(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, long startTime, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    return null;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, MessageId messageId, boolean inclusive, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    return null;
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {

  }

  @Override
  public void rollback(TopicMetadata metadata, RollbackDetail rollbackDetail) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}
