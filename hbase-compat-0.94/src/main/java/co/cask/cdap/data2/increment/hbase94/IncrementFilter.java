/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.increment.hbase94;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Simple filter for increment columns that includes all cell values in a column with a "delta" prefix up to and
 * including the first cell it finds that is not an increment.  This allows the {@link IncrementHandler}
 * coprocessor to return the correct value for an increment column by summing the last total sum plus all newer
 * delta values.
 */
public class IncrementFilter extends FilterBase {
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (IncrementHandler.isIncrement(kv)) {
      // all visible increments should be included until we get to a non-increment
      return ReturnCode.INCLUDE;
    } else {
      // as soon as we find a KV to include we can move to the next column
      return ReturnCode.INCLUDE_AND_NEXT_COL;
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("IncrementFilter is only intended for server-side use!");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("IncrementFilter is only intended for server-side use!");
  }
}
