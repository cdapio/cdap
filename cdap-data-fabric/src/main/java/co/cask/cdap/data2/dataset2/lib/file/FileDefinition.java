/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.File;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Dataset definition for File datasets.
 */
public class FileDefinition implements DatasetDefinition<File, FileAdmin> {

  private final String name;

  /**
   * Constructor with dataset type name.
   * @param name the type name to beused for this dataset definition.
   */
  public FileDefinition(String name) {
    this.name = name;
  }

  @Inject
  private LocationFactory locationFactory;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    Map<String, String> props = properties.getProperties();
    String basePath = props.get(File.PROPERTY_BASE_PATH);
    if (basePath == null) {
      basePath = instanceName.replace('.', '/');
      props = Maps.newHashMap(props);
      props.put(File.PROPERTY_BASE_PATH, basePath);
    }
    return DatasetSpecification.builder(instanceName, getName()).properties(props).build();
  }

  @Override
  public FileAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new FileAdmin(locationFactory, spec);
  }

  @Override
  public File getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    return new FileDataset(spec.getName(), locationFactory, spec.getProperties(), arguments);
  }
}
