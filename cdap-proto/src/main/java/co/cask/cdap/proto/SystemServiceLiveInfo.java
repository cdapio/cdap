/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Live info for a system service.
 */
public class SystemServiceLiveInfo {

  private final List<Containers.ContainerInfo> containers;

  public SystemServiceLiveInfo(List<Containers.ContainerInfo> containers) {
    this.containers = Collections.unmodifiableList(new ArrayList<>(containers));
  }

  @Nullable
  public List<Containers.ContainerInfo> getContainers() {
    return containers;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   *
   */
  public static class Builder {
    private final List<Containers.ContainerInfo> containers = new ArrayList<>();

    public Builder addContainer(Containers.ContainerInfo containerInfo) {
      containers.add(containerInfo);
      return this;
    }

    public SystemServiceLiveInfo build() {
      return new SystemServiceLiveInfo(containers);
    }
  }
}
