/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.app.upgrade;

import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.upgrade.ApplicationUpgradeDetail;
import java.util.List;

/**
 * Manager for all upgrade related operations.
 */
public interface UpgradeManager {

  /**
   * Lists upgrade details for applications in a namespace.
   *
   * @param namespace the namespace in which applications are searched.
   * @return A list of {@link ApplicationUpgradeDetail} containing application and current/latest
   *     version details for the application artifact and the plugins.
   */
  List<ApplicationUpgradeDetail> listUpgrades(NamespaceId namespace) throws Exception;
}
