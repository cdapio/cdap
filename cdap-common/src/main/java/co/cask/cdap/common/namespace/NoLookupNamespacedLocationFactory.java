/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Implementation of {@link NamespacedLocationFactory} that does not perform lookup for {@link NamespaceMeta}.
 */
public class NoLookupNamespacedLocationFactory implements NamespacedLocationFactory {

  private final LocationFactory locationFactory;
  private final String namespaceDir;

  @Inject
  public NoLookupNamespacedLocationFactory(CConfiguration cConf, LocationFactory locationFactory) {
    this.namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    this.locationFactory = locationFactory;
  }

  @Override
  public Location get(NamespaceId namespaceId) throws IOException {
    return locationFactory.create(namespaceDir).append(namespaceId.getNamespace());
  }

  @Override
  public Location get(NamespaceMeta namespaceMeta) throws IOException {
    String rootDirectory = namespaceMeta.getConfig().getRootDirectory();
    if (Strings.isNullOrEmpty(rootDirectory)) {
      // if no custom mapping was specified, then use the default namespaces location
      return get(namespaceMeta.getNamespaceId());
    }
    return locationFactory.create(namespaceMeta.getConfig().getRootDirectory());
  }
}
