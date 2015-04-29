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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Etl Split.
 */
public class EtlSplit extends InputSplit implements Writable {
  private List<CamusRequest> requests = new ArrayList<CamusRequest>();
  private long length = 0;
  private String currentTopic = "";

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      CamusRequest r = new EtlRequest();
      r.readFields(in);
      requests.add(r);
      length += r.estimateDataSize();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(requests.size());
    for (CamusRequest r : requests) {
      r.write(out);
    }
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  public int getNumRequests() {
    return requests.size();
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  public void addRequest(CamusRequest request) {
    requests.add(request);
    length += request.estimateDataSize();
  }

  public CamusRequest popRequest() {
    if (requests.size() > 0) {
      for (int i = 0; i < requests.size(); i++) {
        // return all request for each topic before returning another topic
        if (requests.get(i).getTopic().equals(currentTopic)) {
          return requests.remove(i);
        }
      }
      CamusRequest cr = requests.remove(requests.size() - 1);
      currentTopic = cr.getTopic();
      return cr;
    } else {
      return null;
    }
  }
}
