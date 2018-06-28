/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.nio.ByteBuffer;

/**
 * Key class to get v1 metadata history key information
 */
public final class MdsHistoryKey {
  private static final byte[] ROW_PREFIX = {'h'};

  public static MDSKey getMdsKey(NamespacedEntityId targetId, long time) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(ROW_PREFIX);
    EntityIdKeyHelper.addTargetIdToKey(builder, targetId);
    builder.add(invertTime(time));
    return builder.build();
  }

  static byte[] getHistoryRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(ROW_PREFIX).build();
    return key.getKey();
  }

  private static long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  static long getHistoryTime(byte[] rowKey) {
    return ByteBuffer.wrap(rowKey).getLong(rowKey.length - 8);
  }

  private MdsHistoryKey() {
  }
}
