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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreFacade;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Provider;

import java.io.IOException;
import java.util.Map;
import javax.inject.Inject;

/**
 * Defines the partitioned dataset type. At this time, the partitions are not managed by the
 * partitioned dataset, so all admin is simply on the partition table.
 * TODO rethink this
 */
public class PartitionedFileSetDefinition extends AbstractDatasetDefinition<PartitionedFileSet, DatasetAdmin> {

  private static final String PARTITION_TABLE_NAME = "partitions";
  private static final String FILESET_NAME = "files";

  private final DatasetDefinition<? extends Table, ?> tableDef;
  private final DatasetDefinition<? extends FileSet, ?> filesetDef;

  @Inject
  private Injector injector;

  public PartitionedFileSetDefinition(String name,
                                      DatasetDefinition<? extends FileSet, ?> filesetDef,
                                      DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    Preconditions.checkArgument(filesetDef != null, "FileSet definition is required");
    this.filesetDef = filesetDef;
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(filesetDef.configure(FILESET_NAME, properties),
                tableDef.configure(PARTITION_TABLE_NAME, properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(filesetDef.getAdmin(spec.getSpecification(FILESET_NAME), classLoader),
                                     tableDef.getAdmin(spec.getSpecification(PARTITION_TABLE_NAME), classLoader));
  }

  @Override
  public PartitionedFileSet getDataset(DatasetSpecification spec,
                                       Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    // properties must contain the partitioning
    Partitioning partitioning = PartitionedFileSetProperties.getPartitioning(spec.getProperties());

    // if the arguments do not contain an output location, generate one from the partition key
    if (FileSetArguments.getOutputPath(arguments) == null) {
      PartitionKey key = PartitionedFileSetArguments.getOutputPartitionKey(arguments, partitioning);
      if (key != null) {
        StringBuilder builder = new StringBuilder();
        String sep = "";
        for (String fieldName : partitioning.getFields().keySet()) {
          builder.append(sep).append(key.getField(fieldName).toString());
          sep = "/";
        }
        String path = builder.toString();
        arguments = Maps.newHashMap(arguments);
        FileSetArguments.setOutputPath(arguments, path);
      }
    }
    FileSet fileset = filesetDef.getDataset(spec.getSpecification(FILESET_NAME), arguments, classLoader);
    Table table = tableDef.getDataset(spec.getSpecification(PARTITION_TABLE_NAME), arguments, classLoader);

    return new PartitionedFileSetDataset(spec.getName(), partitioning, fileset, table, spec, arguments,
                                         new Provider<ExploreFacade>() {
                                           @Override
                                           public ExploreFacade get() {
                                             try {
                                               return injector.getInstance(ExploreFacade.class);
                                             } catch (Exception e) {
                                               // since explore is optional for this dataset, ignore
                                               return null;
                                             }
                                           }
                                         });
  }
}
