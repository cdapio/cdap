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

package co.cask.cdap.metrics.process;

import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.proto.id.TopicId;

/**
 * An abstraction on persistent storage of TMS fetch information.
 */
public class MessagingConsumerMetaTable extends AbstractConsumerMetaTable {

  public MessagingConsumerMetaTable(MetricsTable metaTable) {
    super(metaTable);
  }

  @Override
  protected byte[] getKey(Object objectKey) {
    return MessagingUtils.toMetadataRowKey(TopicId.class.cast(objectKey));
  }
}
