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
package co.cask.cdap.data2.transaction.queue;

/**
 * Constants for queue implementation in HBase.
 */
public final class QueueUtils {

  public static String determineQueueConfigTableName(String queueTableName) {
    // the name of this table has the form: <cdap root namespace>.<queue namespace>.<system namespace>.(queue|stream).*
    // beware that the cdap name space may also contain ., but there must be at least two .

    int dotPos = queueTableName.indexOf('.');
    int colonPos = queueTableName.indexOf(':');

    int nsDivider;
    // In hbase 0.96, 0.98, a ':' is used as the namespace divider. In 0.94, a '.' is used.
    if (colonPos >= 0) {
      nsDivider = colonPos;
    } else {
      nsDivider = dotPos;
    }

    if (nsDivider < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }
    int secondDot = queueTableName.indexOf('.', nsDivider + 1);
    if (secondDot < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }
    int qpos = queueTableName.indexOf(QueueConstants.QueueType.QUEUE.toString(), secondDot + 1);
    int spos = queueTableName.indexOf(QueueConstants.QueueType.STREAM.toString(), secondDot + 1);
    int pos;
    if (qpos < 0) {
      pos = spos;
    } else if (spos < 0) {
      pos = qpos;
    } else {
      pos = Math.min(qpos, spos);
    }
    if (pos < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }

    return queueTableName.substring(0, pos) + QueueConstants.QUEUE_CONFIG_TABLE_NAME;
  }

  private QueueUtils() { }
}
