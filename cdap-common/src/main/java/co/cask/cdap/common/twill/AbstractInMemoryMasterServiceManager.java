/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.twill;

import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.SystemServiceLiveInfo;
import com.google.common.collect.ImmutableList;

/**
 * InMemory CDAP Service Management class.
 */
public abstract class AbstractInMemoryMasterServiceManager implements MasterServiceManager {

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    return new SystemServiceLiveInfo(ImmutableList.<Containers.ContainerInfo>of());
  }

  @Override
  public int getInstances() {
    return 1;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return false;
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public int getMaxInstances() {
    return 1;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }

  @Override
  public boolean isServiceEnabled() {
    return true;
  }
}
