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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link NamespacedLocationFactory}
 */
public class DefaultNamespacedLocationFactory implements NamespacedLocationFactory {

  private final LocationFactory locationFactory;
  private final String namespaceDir;

  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public DefaultNamespacedLocationFactory(CConfiguration cConf,
                                          LocationFactory locationFactory,
                                          NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    this.locationFactory = locationFactory;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public Location get(Id.Namespace namespaceId) throws IOException {
    return get(namespaceId, null);
  }

  @Override
  public Location get(NamespaceMeta namespaceMeta) throws IOException {
    String rootDirectory = namespaceMeta.getConfig().getRootDirectory();
    if (Strings.isNullOrEmpty(rootDirectory)) {
      // if no custom mapping was specified the use the default namespaces location
      return getNonCustomMappedLocation(namespaceMeta.getNamespaceId().toId());
    }
    return Locations.getLocationFromAbsolutePath(locationFactory, rootDirectory);
  }

  @Override
  public Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException {
    Location namespaceLocation;
    if (Id.Namespace.DEFAULT.equals(namespaceId) || Id.Namespace.SYSTEM.equals(namespaceId) ||
      Id.Namespace.CDAP.equals(namespaceId)) {
      // since these are cdap reserved namespace we know there cannot be a custom mapping for this.
      // for optimization don't query for namespace meta
      namespaceLocation = getNonCustomMappedLocation(namespaceId);
    } else {
      // since this is not a cdap reserved namespace we look up meta if there is a custom mapping
      NamespaceMeta namespaceMeta;
      try {
        namespaceMeta = namespaceQueryAdmin.get(namespaceId.toEntityId());
      } catch (Exception e) {
        throw new IOException(String.format("Failed to get namespace meta for namespace %s", namespaceId), e);
      }
      namespaceLocation = get(namespaceMeta);
    }

    if (subPath != null) {
      namespaceLocation = namespaceLocation.append(subPath);
    }
    return namespaceLocation;
  }

  /**
   * Gives a namespaced location for a namespace which does not have a custom mapping
   *
   * @param namespaceId the simple cdap namespace
   * @return location of the namespace
   */
  private Location getNonCustomMappedLocation(Id.Namespace namespaceId) throws IOException {
    return locationFactory.create(namespaceDir).append(namespaceId.getId());
  }

  @Override
  public Location getBaseLocation() throws IOException {
    return locationFactory.create("/");
  }
}
