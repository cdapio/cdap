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

package co.cask.cdap.proto;

import com.google.common.collect.Lists;

import java.util.List;

/**
* Live info for distributed mode, adds yarn app id and container information.
*/
public class DistributedProgramLiveInfo extends ProgramLiveInfo implements Containers {

  private final String yarnAppId;
  private final List<Containers.ContainerInfo> containers = Lists.newArrayList();
  private final List<String> services = Lists.newArrayList();

  public DistributedProgramLiveInfo(Id.Program program, ProgramType type, String yarnAppId) {
    super(program, type, "distributed");
    this.yarnAppId = yarnAppId;
  }

  public String getYarnAppId() {
    return yarnAppId;
  }

  @Override
  public List<Containers.ContainerInfo> getContainers() {
    return containers;
  }

  @Override
  public void addContainer(Containers.ContainerInfo container) {
    containers.add(container);
  }

  public void addServices(List<String> services) {
    this.services.addAll(services);
  }
}
