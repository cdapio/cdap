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

package co.cask.cdap.templates.etl.common.kafka;

/**
 *
 */

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;


/**
 *
 */
public class BaseAllocator extends WorkAllocator {

  protected Properties props;

  public void init(Properties props) {
    this.props = props;
  }

  protected void reverseSortRequests(List<CamusRequest> requests) {
    // Reverse sort by size
    Collections.sort(requests, new Comparator<CamusRequest>() {
      @Override
      public int compare(CamusRequest o1, CamusRequest o2) {
        if (o2.estimateDataSize() == o1.estimateDataSize()) {
          return 0;
        }
        if (o2.estimateDataSize() < o1.estimateDataSize()) {
          return -1;
        } else {
          return 1;
        }
      }
    });
  }

  @Override
  public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
    int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);

    reverseSortRequests(requests);

    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

    for (int i = 0; i < numTasks; i++) {
      if (requests.size() > 0) {
        kafkaETLSplits.add(new EtlSplit());
      }
    }

    for (CamusRequest r : requests) {
      getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
    }

    return kafkaETLSplits;
  }

  protected EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException {
    EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

    for (int i = 1; i < kafkaETLSplits.size(); i++) {
      EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
      if ((smallest.getLength() == challenger.getLength() && smallest.getNumRequests() > challenger.getNumRequests())
        || smallest.getLength() > challenger.getLength()) {
        smallest = challenger;
      }
    }

    return smallest;
  }

}
