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

package co.cask.cdap.data2.dataset2.lib.file;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Administration for file sets.
 */
public class FileSetAdmin implements DatasetAdmin, Updatable {

  private final DatasetSpecification spec;
  private final boolean isExternal;
  private final Location baseLocation;
  private final CConfiguration cConf;
  private final DatasetContext datasetContext;
  private final LocationFactory absoluteLocationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;

  FileSetAdmin(DatasetContext datasetContext, CConfiguration cConf,
               LocationFactory absoluteLocationFactory,
               NamespacedLocationFactory namespacedLocationFactory,
               DatasetSpecification spec) throws IOException {

    this.spec = spec;
    this.isExternal = FileSetProperties.isDataExternal(spec.getProperties());
    this.baseLocation = FileSetDataset.determineBaseLocation(datasetContext, cConf, spec,
                                                             absoluteLocationFactory, namespacedLocationFactory);
    this.datasetContext = datasetContext;
    this.cConf = cConf;
    this.absoluteLocationFactory = absoluteLocationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
  }

  @Override
  public boolean exists() throws IOException {
    return baseLocation.isDirectory();
  }

  @Override
  public void create() throws IOException {
    if (!isExternal) {
      if (exists()) {
        throw new IOException(String.format(
          "Base location for file set '%s' at %s already exists", spec.getName(), baseLocation));
      }
      String permissions = spec.getProperties().get(DatasetProperties.PROPERTY_PERMISSIONS);
      String group = spec.getProperties().get(DatasetProperties.PROPERTY_PERMISSIONS_GROUP);
      group = group != null ? group : UserGroupInformation.getCurrentUser().getPrimaryGroupName();

      // we can't simply mkdirs() the base location, because we need to set the group id on
      // every directory we create. Thus find the first ancestor of the base that does not exist:
      Location ancestor = baseLocation;
      Location firstDirToCreate = null;
      while (ancestor != null && !ancestor.exists()) {
        firstDirToCreate = ancestor;
        ancestor = Locations.getParent(ancestor);
      }
      // it is unlikely to be null: only if it was created after the exists() call above
      if (firstDirToCreate != null) {
        if (null == permissions) {
          firstDirToCreate.mkdirs();
          firstDirToCreate.setGroup(group);
          // all following directories are created with the same group id as their parent
          baseLocation.mkdirs();
        } else {
          firstDirToCreate.mkdirs(permissions);
          firstDirToCreate.setGroup(group);
          // all following directories are created with the same group id as their parent
          baseLocation.mkdirs(permissions);
        }
      }
    } else if (!exists()) {
        throw new IOException(String.format(
          "Base location for external file set '%s' at %s does not exist", spec.getName(), baseLocation));
    }
  }

  @Override
  public void drop() throws IOException {
    if (!isExternal) {
      baseLocation.delete(true);
    }
  }

  @Override
  public void truncate() throws IOException {
    drop();
    create();
  }

  @Override
  public void upgrade() throws IOException {
    // nothing to do
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

  @Override
  public void update(DatasetSpecification oldSpec) throws IOException {
    // we don't allow changing the location for an internal file set
    // the only way the baseLocation can change is that it is externalized to an outside location
    // in this case, we have already validated the location is outside (via determineBaseLocation)
    // all we need to do is therefore to move it to the new base location if that location has changed
    if (isExternal && !FileSetProperties.isDataExternal(oldSpec.getProperties())) {
      Location oldBaseLocation = FileSetDataset.determineBaseLocation(
        datasetContext, cConf, oldSpec, absoluteLocationFactory, namespacedLocationFactory);
      if (!baseLocation.equals(oldBaseLocation)) {
        oldBaseLocation.renameTo(baseLocation);
      }
    }
  }
}
