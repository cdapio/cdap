/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.service.LiveInfo;
import com.google.common.collect.Lists;

import java.util.List;

/**
* Live info for distributed mode, adds yarn app id and container information.
*/
public class DistributedLiveInfo extends LiveInfo implements Containers {

  private final String yarnAppId;
  private final List<Containers.ContainerInfo> containers = Lists.newArrayList();

  DistributedLiveInfo(Id.Program program, Type type, String yarnAppId) {
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

}
