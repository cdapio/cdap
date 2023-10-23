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
 *
 */

package io.cdap.cdap.internal.provision;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Context for initializing a provisioner.
 */
public class DefaultSystemProvisionerContext implements ProvisionerSystemContext {

  private final String prefix;
  private final CConfiguration cConf;
  private final AtomicReference<Map<String, String>> properties;
  private final String cdapVersion;
  private final Map<String, Lock> locks;
  private final long confReloadInterval;
  private long lastConfReloadTime;

  DefaultSystemProvisionerContext(CConfiguration cConf, String provisionerName) {
    this.prefix = String.format("%s%s.", Constants.Provisioner.SYSTEM_PROPERTY_PREFIX,
        provisionerName);
    this.cConf = CConfiguration.copy(cConf);
    this.properties = new AtomicReference<>(Collections.emptyMap());
    this.cdapVersion = ProjectInfo.getVersion().toString();
    this.locks = new ConcurrentHashMap<>();
    this.confReloadInterval = cConf.getLong(Constants.Provisioner.RELOAD_INTERVAL);
    this.lastConfReloadTime = 0;

    reloadProperties();
  }

  @Override
  public Map<String, String> getProperties() {
    return properties.get();
  }

  @Override
  public synchronized void reloadProperties() {
    if (confReloadInterval <= 0
        || System.currentTimeMillis() - confReloadInterval > lastConfReloadTime) {
      cConf.reloadConfiguration();
      properties.set(Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix)));
      lastConfReloadTime = System.currentTimeMillis();
    }
  }

  @Override
  public String getCDAPVersion() {
    return cdapVersion;
  }

  @Override
  public Lock getLock(String name) {
    return locks.computeIfAbsent(name, n -> new ReentrantLock());
  }
}
