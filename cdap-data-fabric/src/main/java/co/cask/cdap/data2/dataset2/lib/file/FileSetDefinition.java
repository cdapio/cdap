/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.file;

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Dataset definition for File datasets.
 */
public class FileSetDefinition implements DatasetDefinition<FileSet, FileSetAdmin> {

  @Inject
  private RootLocationFactory absoluteLocationFactory;

  @Inject
  private NamespacedLocationFactory namespacedLocationFactory;

  @Inject
  private CConfiguration cConf;

  private final String name;

  /**
   * Constructor with dataset type name.
   * @param name the type name to be used for this dataset definition.
   */
  public FileSetDefinition(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName()).properties(properties.getProperties()).build();
  }

  @Override
  public FileSetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new FileSetAdmin(datasetContext, cConf, absoluteLocationFactory, namespacedLocationFactory, spec);
  }

  @Override
  public FileSet getDataset(DatasetContext datasetContext, DatasetSpecification spec, Map<String, String> arguments,
                            ClassLoader classLoader) throws IOException {
    return new FileSetDataset(datasetContext, cConf, spec, absoluteLocationFactory, namespacedLocationFactory,
                              arguments == null ? Collections.<String, String>emptyMap() : arguments);
  }
}
