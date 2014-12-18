/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive.table;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Record reader for table splits.
 */
public class TableRecordReader implements RecordReader<Void, ObjectWritable> {
  private final Scanner scanner;

  public TableRecordReader(TableInputSplit split, Table table) {
    this.scanner = table.scan(split.getStartKey(), split.getStopKey());
  }

  @Override
  public boolean next(Void key, ObjectWritable value) throws IOException {
    Row row = scanner.next();
    if (row == null) {
      return false;
    }
    value.set(row);
    return true;
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public ObjectWritable createValue() {
    return new ObjectWritable();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
