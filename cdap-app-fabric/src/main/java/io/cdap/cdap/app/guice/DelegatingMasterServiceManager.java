/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import io.cdap.cdap.common.twill.MasterServiceManager;
import io.cdap.cdap.proto.SystemServiceLiveInfo;
import org.apache.twill.api.logging.LogEntry;

import java.util.Map;
import java.util.Set;

/**
 * A {@link MasterServiceManager} that delegates all methods to another {@link MasterServiceManager}.
 */
class DelegatingMasterServiceManager implements MasterServiceManager {

  private final MasterServiceManager delegate;

  DelegatingMasterServiceManager(MasterServiceManager delegate) {
    this.delegate = delegate;
  }

  MasterServiceManager getDelegate() {
    return delegate;
  }

  @Override
  public boolean isServiceEnabled() {
    return getDelegate().isServiceEnabled();
  }

  @Override
  public String getDescription() {
    return getDelegate().getDescription();
  }

  @Override
  public int getInstances() {
    return getDelegate().getInstances();
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return getDelegate().setInstances(instanceCount);
  }

  @Override
  public int getMinInstances() {
    return getDelegate().getMinInstances();
  }

  @Override
  public int getMaxInstances() {
    return getDelegate().getMaxInstances();
  }

  @Override
  public boolean isLogAvailable() {
    return getDelegate().isLogAvailable();
  }

  @Override
  public boolean isServiceAvailable() {
    return getDelegate().isServiceAvailable();
  }

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    return getDelegate().getLiveInfo();
  }

  @Override
  public void restartAllInstances() {
    getDelegate().restartAllInstances();
  }

  @Override
  public void restartInstances(int instanceId, int... moreInstanceIds) {
    getDelegate().restartInstances(instanceId, moreInstanceIds);
  }

  @Override
  public void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    getDelegate().updateServiceLogLevels(logLevels);
  }

  @Override
  public void resetServiceLogLevels(Set<String> loggerNames) {
    getDelegate().resetServiceLogLevels(loggerNames);
  }
}
