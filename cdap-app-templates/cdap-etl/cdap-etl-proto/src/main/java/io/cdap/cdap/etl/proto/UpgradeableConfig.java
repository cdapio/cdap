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

package co.cask.cdap.etl.proto;

/**
 * Upgradeable config for ETL applications, which allows chain upgrading until the latest version is reached.
 *
 * @param <T> type of config it gets upgraded to.
 */
public interface UpgradeableConfig<T extends UpgradeableConfig> {

  /**
   * @return whether or not this config can be updated
   */
  boolean canUpgrade();

  /**
   * Return an upgraded config, which may be upgradeable itself.
   * This allows v0 to upgrade to v1, which can then be upgraded to v2, and so on.
   * In this way, as long as each config version knows how to upgrade to the next version,
   * we will be able to upgrade from any old version to the most current version.
   *
   * @param upgradeContext context for upgrading
   * @return the upgraded config
   */
  T upgrade(UpgradeContext upgradeContext);
}
