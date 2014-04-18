package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.service.LiveInfo;
import com.google.common.collect.Lists;

import java.util.List;

/**
* Live info for distributed mode, adds yarn app id and container information.
*/
public class DistributedLiveInfo extends LiveInfo {

  private final String yarnAppId;
  private final List<ContainerInfo> containers = Lists.newArrayList();

  DistributedLiveInfo(Id.Program program, Type type, String yarnAppId) {
    super(program, type, "distributed");
    this.yarnAppId = yarnAppId;
  }

  public String getYarnAppId() {
    return yarnAppId;
  }

  public List<ContainerInfo> getContainers() {
    return containers;
  }

  void addContainer(ContainerInfo container) {
    containers.add(container);
  }

  static class ContainerInfo {
    private final String type;
    private final String name;
    private final int instance;
    private final String container;
    private final String host;
    private final int memory;
    private final int virtualCores;
    private final Integer debugPort;

    ContainerInfo(ContainerType type, String name, int instance, String container,
                  String host, int memory, int virtualCores, Integer debugPort) {
      this.type = type.name().toLowerCase();
      this.name = name;
      this.instance = instance;
      this.container = container;
      this.host = host;
      this.memory = memory;
      this.virtualCores = virtualCores;
      this.debugPort = debugPort;
    }

    public ContainerType getType() {
      return ContainerType.valueOf(type.toUpperCase());
    }

    public String getName() {
      return name;
    }

    public int getInstance() {
      return instance;
    }

    public String getContainer() {
      return container;
    }

    public String getHost() {
      return host;
    }

    public int getMemory() {
      return memory;
    }

    public int getVirtualCores() {
      return virtualCores;
    }

    public Integer getDebugPort() {
      return debugPort;
    }
  }

  enum ContainerType { FLOWLET, PROCEDURE }
}
