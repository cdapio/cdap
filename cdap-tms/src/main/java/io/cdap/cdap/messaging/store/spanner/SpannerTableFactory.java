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

package io.cdap.cdap.messaging.store.spanner;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.spi.data.TableSchemaIncompatibleException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.storage.spanner.SpannerStorageProvider;
import java.io.IOException;

public class SpannerTableFactory implements TableFactory {

  SpannerStorageProvider provider;

  @Inject
  public SpannerTableFactory(CConfiguration cConf, SpannerStorageProvider provider) {
    this.provider = provider;
  }

  @Override
  public MetadataTable createMetadataTable() throws IOException {
    try {
      provider.getStructuredTableAdmin().createOrUpdate(SpannerMessageTable.METADATA_TABLE_SPEC);
    } catch (TableSchemaIncompatibleException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public MessageTable createMessageTable(TopicMetadata topicMetadata) throws IOException {
    try {
      provider.getStructuredTableAdmin().createOrUpdate(SpannerMessageTable.MESSAGE_TABLE_SPEC);
    } catch (TableSchemaIncompatibleException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
