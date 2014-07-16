/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Map;

/**
 * Iterable for {@link com.continuuity.metrics.data.TimeValue} from single row.
 */
final class TimeValueIterable implements Iterable<TimeValue> {

  private final long timeBase;
  private final int resolution;
  private final long startTime;
  private final long endTime;
  private final Iterable<Map.Entry<byte[], byte[]>> iterable;

  TimeValueIterable(long timeBase, int resolution, long startTime, long endTime,
                    Iterable<Map.Entry<byte[], byte[]>> iterable) {
    this.timeBase = timeBase;
    this.resolution = resolution;
    this.startTime = startTime;
    this.endTime = endTime;
    this.iterable = ImmutableList.copyOf(iterable);
  }

  @Override
  public Iterator<TimeValue> iterator() {
    final Iterator<Map.Entry<byte[], byte[]>> iterator = iterable.iterator();

    return new AbstractIterator<TimeValue>() {
      @Override
      protected TimeValue computeNext() {
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          long timestamp = timeBase + (long) Bytes.toShort(entry.getKey()) * resolution;
          if (timestamp < startTime) {
            continue;
          }
          if (timestamp > endTime) {
            return endOfData();
          }
          return new TimeValue(timestamp, Bytes.toInt(entry.getValue()));
        }
        return endOfData();
      }
    };
  }
}
