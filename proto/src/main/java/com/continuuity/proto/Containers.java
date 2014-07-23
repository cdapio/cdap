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

package com.continuuity.proto;

import java.util.List;

/**
 * Containers define methods to get and add information about YARN containers.
 */
public interface Containers {

  /**
   * @return List of {@link ContainerInfo}.
   */
  public List<ContainerInfo> getContainers();

  /**
   * Add {@link ContainerInfo}.
   *
   * @param container instance of {@link ContainerInfo}.
   */
  public void addContainer(Containers.ContainerInfo container);

  /**
   * ContainerTypes - Flowlet, Procedure and Service
   */
  enum ContainerType { FLOWLET, PROCEDURE, SERVICE }

  /**
   * POJO holding information about container running in YARN.
   */
  public static final class ContainerInfo {
    private final String type;
    private final String name;
    private final int instance;
    private final String container;
    private final String host;
    private final int memory;
    private final int virtualCores;
    private final Integer debugPort;

    public ContainerInfo(ContainerType type, String name, int instance, String container,
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
}
