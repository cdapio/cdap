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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Implementation of {@link NamespacedLocationFactory} to be only used in unit tests. This implementation does not
 * perform lookup for {@link NamespaceMeta} like {@link DefaultNamespacedLocationFactory} does and hence allow unit
 * tests to use it without creating namespace meta for the namespace.
 */
public class NamespacedLocationFactoryTestClient implements NamespacedLocationFactory {

  private final LocationFactory locationFactory;
  private final String namespaceDir;

  @Inject
  public NamespacedLocationFactoryTestClient(CConfiguration cConf, LocationFactory locationFactory) {
    this.namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    this.locationFactory = locationFactory;
  }

  @Override
  public Location get(Id.Namespace namespaceId) throws IOException {
    return get(namespaceId, null);
  }

  @Override
  public Location get(NamespaceMeta namespaceMeta) throws IOException {
    return get(namespaceMeta.getNamespaceId().toId(), null);
  }

  @Override
  public Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException {
    Location namespaceLocation = locationFactory.create(namespaceDir).append(namespaceId.getId());
    if (subPath != null) {
      namespaceLocation = namespaceLocation.append(subPath);
    }
    return namespaceLocation;
  }

  @Override
  public Location getBaseLocation() throws IOException {
    return locationFactory.create("/");
  }
}
