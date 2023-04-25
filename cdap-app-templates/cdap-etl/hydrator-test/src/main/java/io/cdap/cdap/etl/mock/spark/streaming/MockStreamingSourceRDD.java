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

package io.cdap.cdap.etl.mock.spark.streaming;

import io.cdap.cdap.api.data.format.StructuredRecord;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;

/**
 * RDD that returns data from a list.
 */
public class MockStreamingSourceRDD extends RDD<StructuredRecord> {

  private final Iterator<StructuredRecord> recordIterator;

  public MockStreamingSourceRDD(SparkContext sc, List<StructuredRecord> recordList) {
    super(sc, scala.collection.JavaConverters.asScalaBuffer(Collections.emptyList()),
        scala.reflect.ClassTag$.MODULE$.apply(String.class));
    recordIterator = new StructuredRecordIterator(recordList);
  }

  @Override
  public Partition[] getPartitions() {
    Partition[] partitions = new Partition[1];
    partitions[0] = () -> 0;
    return partitions;
  }

  @Override
  public Iterator<StructuredRecord> compute(Partition split, TaskContext context) {
    return recordIterator;
  }

  /**
   * StructuredRecordIterator over a list of records.
   */
  private static class StructuredRecordIterator implements Iterator<StructuredRecord>,
      Serializable {

    private final List<StructuredRecord> recordList;
    private int index = 0;
    public StructuredRecordIterator(List<StructuredRecord> recordList) {
      this.recordList = recordList;
    }

    @Override
    public boolean hasNext() {
      return index < recordList.size();
    }

    @Override
    public StructuredRecord next() {
      int current = index++;
      return recordList.get(current);
    }
  }
}

