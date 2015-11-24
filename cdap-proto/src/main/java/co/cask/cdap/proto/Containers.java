/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import java.util.List;
import javax.annotation.Nullable;

/**
 * Containers define methods to get and add information about YARN containers.
 */
public interface Containers {

  /**
   * @return List of {@link ContainerInfo}.
   */
  List<ContainerInfo> getContainers();

  /**
   * Add {@link ContainerInfo}.
   *
   * @param container instance of {@link ContainerInfo}.
   */
  void addContainer(Containers.ContainerInfo container);

  /**
   * ContainerTypes - System Service, Flowlet and Service
   */
  enum ContainerType { SYSTEM_SERVICE, FLOWLET, SERVICE }

  /**
   * POJO holding information about container running in YARN.
   */
  final class ContainerInfo {
    private final String type;
    private final String name;
    private final Integer instance;
    private final String container;
    private final String host;
    private final Integer memory;
    private final Integer virtualCores;
    private final Integer debugPort;

    public ContainerInfo(ContainerType type, String name, @Nullable Integer instance, @Nullable String container,
                  String host, @Nullable Integer memory, @Nullable Integer virtualCores, @Nullable Integer debugPort) {
      this.type = type.name().toLowerCase();
      this.name = name;
      this.instance = instance;
      this.container = container;
      this.host = host;
      this.memory = memory;
      this.virtualCores = virtualCores;
      this.debugPort = debugPort;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ContainerInfo{");
      sb.append("type='").append(type).append('\'');
      sb.append(", name='").append(name).append('\'');
      sb.append(", instance=").append(instance);
      sb.append(", container='").append(container).append('\'');
      sb.append(", host='").append(host).append('\'');
      sb.append(", memory=").append(memory);
      sb.append(", virtualCores=").append(virtualCores);
      sb.append(", debugPort=").append(debugPort);
      sb.append('}');
      return sb.toString();
    }

    public ContainerType getType() {
      return ContainerType.valueOf(type.toUpperCase());
    }

    public String getName() {
      return name;
    }

    @Nullable
    public Integer getInstance() {
      return instance;
    }

    @Nullable
    public String getContainer() {
      return container;
    }

    public String getHost() {
      return host;
    }

    @Nullable
    public Integer getMemory() {
      return memory;
    }

    @Nullable
    public Integer getVirtualCores() {
      return virtualCores;
    }

    @Nullable
    public Integer getDebugPort() {
      return debugPort;
    }
  }
}
