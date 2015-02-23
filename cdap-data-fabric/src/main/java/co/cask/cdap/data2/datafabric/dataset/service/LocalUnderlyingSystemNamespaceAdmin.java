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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Manages namespaces on local underlying systems.
 */
public final class LocalUnderlyingSystemNamespaceAdmin extends UnderlyingSystemNamespaceAdmin {

  @Inject
  public LocalUnderlyingSystemNamespaceAdmin(CConfiguration cConf, LocationFactory locationFactory) {
    super(cConf, locationFactory);
  }
}
