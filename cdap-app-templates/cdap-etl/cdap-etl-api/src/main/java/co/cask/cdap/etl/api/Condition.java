/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;

/**
 * A condition that must be fulfilled.
 */
public class Condition {

  private static final Logger LOG = LoggerFactory.getLogger(Condition.class);
  private static final Gson GSON = new Gson();

  /**
   * Type of condition.
   */
  public enum Type {
    /**
     * Properties:
     * - datasets
     * - partition
     */
    PARTITION_EXISTS,
    TRUE,
    FALSE
  }

  private final Type type;
  private final Map<String, String> properties;

  public Condition(Type type, Map<String, String> properties) {
    this.type = type;
    this.properties = properties;
  }

  public Condition(Type type) {
    this(type, ImmutableMap.<String, String>of());
  }

  public Type getType() {
    return type;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean check(MapReduceContext context) throws Exception {
    // TODO: move implementation out to plugin
    long logicalStartTime = context.getLogicalStartTime();
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.setTimeInMillis(logicalStartTime);

    switch (type) {
      case PARTITION_EXISTS:
        PartitionKey key = parsePartitionString(calendar, properties.get("partition"));
        LOG.warn("REMOVE: Partition: {}", key.toString());
        Iterable<String> datasets = Splitter.on(",").split(properties.get("datasets"));
        for (String dataset : datasets) {
          PartitionedFileSet pfs = context.getDataset(dataset);
          PartitionOutput output = pfs.getPartitionOutput(key);
          Location outputLocation = output.getLocation();

          if (!outputLocation.exists()) {
            LOG.warn("REMOVE: Partition doesn't exist: {}", key.toString());
            return false;
          }
        }
        return true;
      case TRUE: return true;
      case FALSE: return false;
    }
    return false;
  }

  static PartitionKey parsePartitionString(Calendar calendar, String partitionString) {
    LOG.warn("REMOVE: parsing partition string: {}", partitionString);
    Map<String, String> partition = GSON.fromJson(partitionString, new TypeToken<Map<String, String>>() { }.getType());
    PartitionKey.Builder keyBuilder = PartitionKey.builder();
    for (Map.Entry<String, String> entry : partition.entrySet()) {
      keyBuilder.addField(entry.getKey(), replaceVariables(calendar, entry.getValue()));
    }
    return keyBuilder.build();
  }

  static String replaceVariables(Calendar calendar, String input) {
    return input
      .replaceAll("\\$\\{start.year\\}", Integer.toString(calendar.get(Calendar.YEAR)))
      .replaceAll("\\$\\{start.month\\}", Integer.toString(calendar.get(Calendar.MONTH)))
      .replaceAll("\\$\\{start.day\\}", Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)))
      .replaceAll("\\$\\{start.hour\\}", Integer.toString(calendar.get(Calendar.HOUR_OF_DAY)))
      .replaceAll("\\$\\{start.minute\\}", Integer.toString(calendar.get(Calendar.MINUTE)))
      .replaceAll("\\$\\{start.second\\}", Integer.toString(calendar.get(Calendar.SECOND)));
  }
}
