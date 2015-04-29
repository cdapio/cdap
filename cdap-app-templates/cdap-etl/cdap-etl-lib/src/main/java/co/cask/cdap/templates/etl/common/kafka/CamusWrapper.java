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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @author kgoodhop
 *
 * @param <R> The type of decoded payload
 */
public class CamusWrapper<R> {
  private R record;
  private long timestamp;
  private MapWritable partitionMap;

  public CamusWrapper(R record) {
    this(record, System.currentTimeMillis());
  }

  public CamusWrapper(R record, long timestamp) {
    this(record, timestamp, "unknown_server", "unknown_service");
  }

  public CamusWrapper(R record, long timestamp, String server, String service) {
    this.record = record;
    this.timestamp = timestamp;
    this.partitionMap = new MapWritable();
    partitionMap.put(new Text("server"), new Text(server));
    partitionMap.put(new Text("service"), new Text(service));
  }

  /**
   * Returns the payload record for a single message
   * @return
   */
  public R getRecord() {
    return record;
  }

  /**
   * Returns current if not set by the decoder
   * @return
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Add a value for partitions
   */
  public void put(Writable key, Writable value) {
    partitionMap.put(key, value);
  }

  /**
   * Get a value for partitions
   * @return the value for the given key
   */
  public Writable get(Writable key) {
    return partitionMap.get(key);
  }

  /**
   * Get all the partition key/partitionMap
   */
  public MapWritable getPartitionMap() {
    return partitionMap;
  }

}
