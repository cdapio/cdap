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
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link NamespacedLocationFactory}
 */
public class DefaultNamespacedLocationFactory implements NamespacedLocationFactory {

  // we need the RootLocationFactory because we want to work with the root of filesystem for custom namespace mapping
  // for example if the custom mapping is /user/someuser/ this requires locationfactory which works with root.
  private final RootLocationFactory locationFactory;
  private final String hdfsNamespace;
  private final String namespaceDir;

  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public DefaultNamespacedLocationFactory(CConfiguration cConf, RootLocationFactory locationFactory,
                                          NamespaceQueryAdmin namespaceQueryAdmin) {
    this.hdfsNamespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
    this.namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    this.locationFactory = locationFactory;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public Location get(Id.Namespace namespaceId) throws IOException {
    return get(namespaceId, null);
  }

  @Override
  public Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException {
    String hdfsDirectory;
    try {
      hdfsDirectory = namespaceQueryAdmin.get(namespaceId).getConfig().getRootDirectory();
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get namespaced location for namespace %s", namespaceId), e);
    }
    Location namespaceLocation;
    if (Strings.isNullOrEmpty(hdfsDirectory)) {
      // if no custom mapping was specified the use the default namespaces location on hdfs
      namespaceLocation = locationFactory.create(hdfsNamespace).append(namespaceDir).append(namespaceId.getId());
    } else {
      // custom mapping are expected to be given from the root of the filesystem.
      namespaceLocation = getBaseLocation().append(hdfsDirectory);
    }
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
