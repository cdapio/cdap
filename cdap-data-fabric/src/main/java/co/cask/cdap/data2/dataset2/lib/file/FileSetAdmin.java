/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Administration for file sets.
 */
public class FileSetAdmin implements DatasetAdmin, Updatable {

  private static final Logger LOG = LoggerFactory.getLogger(FileSetAdmin.class);

  private final DatasetSpecification spec;
  private final boolean isExternal;
  private final boolean useExisting;
  private final boolean possessExisting;
  private final Location baseLocation;
  private final CConfiguration cConf;
  private final DatasetContext datasetContext;
  private final LocationFactory locationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;

  FileSetAdmin(DatasetContext datasetContext, CConfiguration cConf,
               LocationFactory locationFactory,
               NamespacedLocationFactory namespacedLocationFactory,
               DatasetSpecification spec) throws IOException {

    this.spec = spec;
    this.isExternal = FileSetProperties.isDataExternal(spec.getProperties());
    this.useExisting = FileSetProperties.isUseExisting(spec.getProperties());
    this.possessExisting = FileSetProperties.isPossessExisting(spec.getProperties());
    this.baseLocation = FileSetDataset.determineBaseLocation(datasetContext, cConf, spec,
                                                             locationFactory, namespacedLocationFactory);
    this.datasetContext = datasetContext;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
  }

  @Override
  public boolean exists() throws IOException {
    return baseLocation.isDirectory();
  }

  @Override
  public void create() throws IOException {
    if (isExternal) {
      validateExists(FileSetProperties.DATA_EXTERNAL);
    } else if (useExisting) {
      validateExists(FileSetProperties.DATA_USE_EXISTING);
    } else if (possessExisting) {
      validateExists(FileSetProperties.DATA_POSSESS_EXISTING);
    } else {
      if (exists()) {
        throw new IOException(String.format(
          "Base location for file set '%s' at %s already exists", spec.getName(), baseLocation));
      }
      String permissions = FileSetProperties.getFilePermissions(spec.getProperties());
      String group = FileSetProperties.getFileGroup(spec.getProperties());
      if (group == null) {
        String[] groups = UserGroupInformation.getCurrentUser().getGroupNames();
        if (groups.length > 0) {
          group = groups[0];
        }
      }

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
        } else {
          firstDirToCreate.mkdirs(permissions);
        }
        if (group != null) {
          try {
            firstDirToCreate.setGroup(group);
          } catch (Exception e) {
            LOG.warn("Failed to set group {} for base location {} of file set {}: {}. Please set it manually.",
                     group, firstDirToCreate.toURI().toString(), spec.getName(), e.getMessage());
          }
        }
        // all following directories are created with the same group id as their parent
        if (null == permissions) {
          baseLocation.mkdirs();
        } else {
          baseLocation.mkdirs(permissions);
        }
      }
    }
  }

  private void validateExists(String property) throws IOException {
    if (!exists()) {
      throw new IOException(String.format(
        "Property '%s' for file set '%s' is true, but base location at %s does not exist",
        property, spec.getName(), baseLocation));
    }
  }

  @Override
  public void drop() throws IOException {
    if (!isExternal && !useExisting) {
      baseLocation.delete(true);
    }
  }

  @Override
  public void truncate() throws IOException {
    if (!isExternal && !useExisting) {
      // we can't simply delete and recreate the base location, because it may have pre-existed:
      // - we might create it with different permissions/ownership than the original
      // - we might not even have permission to create (write in the parent dir)
      // Hence: delete all contents (all children) of the base location.
      for (Location child : baseLocation.list()) {
        child.delete(true);
      }
    }
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
        datasetContext, cConf, oldSpec, locationFactory, namespacedLocationFactory);
      if (!baseLocation.equals(oldBaseLocation)) {
        oldBaseLocation.renameTo(baseLocation);
      }
    }
  }
}
