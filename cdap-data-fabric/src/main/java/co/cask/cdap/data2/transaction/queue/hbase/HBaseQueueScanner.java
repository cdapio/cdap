/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

/**
 * An adapter class to convert HBase {@link ResultScanner} into {@link QueueScanner}.
 */
final class HBaseQueueScanner implements QueueScanner {
  private final ResultScanner scanner;
  private final LinkedList<Result> cached = Lists.newLinkedList();
  private final int numRows;
  private final Function<byte[], byte[]> rowKeyConverter;

  public HBaseQueueScanner(ResultScanner scanner, int numRows, Function<byte[], byte[]> rowKeyConverter) {
    this.scanner = scanner;
    this.numRows = numRows;
    this.rowKeyConverter = rowKeyConverter;
  }

  @Override
  public ImmutablePair<byte[], Map<byte[], byte[]>> next() throws IOException {
    while (true) {
      if (cached.size() > 0) {
        Result result = cached.removeFirst();
        Map<byte[], byte[]> row = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
        return ImmutablePair.of(rowKeyConverter.apply(result.getRow()), row);
      }
      Result[] results = scanner.next(numRows);
      if (results.length == 0) {
        return null;
      }
      Collections.addAll(cached, results);
    }
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }
}
