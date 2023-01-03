/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSetArguments;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Defines the partitioned dataset type. At this time, the partitions are not managed by the
 * partitioned dataset, so all admin is simply on the partition table.
 */
public class TimePartitionedFileSetDefinition extends PartitionedFileSetDefinition {

  public TimePartitionedFileSetDefinition(String name,
                                          DatasetDefinition<? extends FileSet, ?> filesetDef,
                                          DatasetDefinition<? extends IndexedTable, ?> tableDef) {
    super(name, filesetDef, tableDef);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    // add the partition key to the properties.
    properties = PartitionedFileSetProperties
      .builder()
      .setPartitioning(TimePartitionedFileSetDataset.PARTITIONING)
      .addAll(properties.getProperties())
      .build();
    return super.configure(instanceName, properties);
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties properties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    // add the partition key to the properties.
    properties = PartitionedFileSetProperties
      .builder()
      .setPartitioning(TimePartitionedFileSetDataset.PARTITIONING)
      .addAll(properties.getProperties())
      .build();
    return super.reconfigure(instanceName, properties, currentSpec);
  }

  @Override
  public PartitionedFileSet getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                       Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    // make any necessary updates to the arguments
    arguments = updateArgumentsIfNeeded(arguments);

    FileSet fileset = filesetDef.getDataset(datasetContext, spec.getSpecification(FILESET_NAME),
                                            arguments, classLoader);
    IndexedTable table = indexedTableDef.getDataset(datasetContext, spec.getSpecification(PARTITION_TABLE_NAME),
                                                    arguments, classLoader);

    return new TimePartitionedFileSetDataset(datasetContext, spec.getName(), fileset, table, spec, arguments);
  }

  // if the arguments do not contain an output path, but an output partition time, generate an output path from that;
  // also convert the output partition time to a partition key and add it to the arguments;
  // also call the super class' method to update arguments if it needs to
  protected Map<String, String> updateArgumentsIfNeeded(Map<String, String> arguments) {
    Long time = TimePartitionedFileSetArguments.getOutputPartitionTime(arguments);
    if (time != null) {
      // set the output path according to partition time
      if (FileSetArguments.getOutputPath(arguments) == null) {
        String outputPathFormat = TimePartitionedFileSetArguments.getOutputPathFormat(arguments);
        String path;
        if (Strings.isNullOrEmpty(outputPathFormat)) {
          path = String.format("%tF/%tH-%tM.%d", time, time, time, time);
        } else {
          SimpleDateFormat format = new SimpleDateFormat(outputPathFormat);
          String timeZoneID = TimePartitionedFileSetArguments.getOutputPathTimeZone(arguments);
          if (!Strings.isNullOrEmpty(timeZoneID)) {
            format.setTimeZone(TimeZone.getTimeZone(timeZoneID));
          }
          path = format.format(new Date(time));
        }
        arguments = Maps.newHashMap(arguments);
        FileSetArguments.setOutputPath(arguments, path);
      }
      // add the corresponding partition key to the arguments
      PartitionKey outputKey = TimePartitionedFileSetDataset.partitionKeyForTime(time);
      PartitionedFileSetArguments.setOutputPartitionKey(arguments, outputKey);
    }
    // delegate to super class for anything it needs to do
    return updateArgumentsIfNeeded(arguments, TimePartitionedFileSetDataset.PARTITIONING);
  }
}
