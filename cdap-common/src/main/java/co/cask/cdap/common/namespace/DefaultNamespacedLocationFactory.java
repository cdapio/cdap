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
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link NamespacedLocationFactory}
 */
public class DefaultNamespacedLocationFactory implements NamespacedLocationFactory {

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;

  @Inject
  public DefaultNamespacedLocationFactory(CConfiguration cConf, LocationFactory locationFactory) {
    this.cConf = cConf;
    this.locationFactory = locationFactory;
  }

  @Override
  public Map<Id.Namespace, Location> list() throws IOException {
    String namespacesDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    Location namespacesLocation = locationFactory.create(namespacesDir);
    Map<Id.Namespace, Location> namespaceLocations = Maps.newHashMap();
    if (namespacesLocation.exists()) {
      for (Location namespaceLocation : namespacesLocation.list()) {
        namespaceLocations.put(Id.Namespace.from(namespaceLocation.getName()), namespaceLocation);
      }
    }
    return namespaceLocations;
  }

  @Override
  public Location get(Id.Namespace namespaceId) throws IOException {
    return get(namespaceId, null);
  }

  @Override
  public Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException {
    String namespacesDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    Location namespaceLocation = locationFactory.create(namespacesDir).append(namespaceId.getId());
    if (subPath != null) {
      namespaceLocation.append(subPath);
    }
    return namespaceLocation;
  }

  @Override
  public Location getBaseLocation() throws IOException {
    return locationFactory.create("/");
  }
}
