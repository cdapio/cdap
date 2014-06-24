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
