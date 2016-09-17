/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;

/**
 * Utility class to generate the service discoverable name (used for registering and discovering service endpoints in
 * ZooKeeper)
 */
public final class ServiceDiscoverable {

  public static String getName(ProgramId programId) {
    return getName(programId.getNamespace(), programId.getApplication(), programId.getProgram());
  }

  public static String getName(String namespaceId, String applicationId, String serviceId) {
    return String.format("%s.%s.%s.%s", ProgramType.SERVICE.name().toLowerCase(), namespaceId,
                         applicationId, serviceId);
  }

  public static ProgramId getId(String name) {
    String[] parts = name.split("\\.");
    Preconditions.checkArgument(parts.length == 4);
    String namespaceId = parts[1];
    String appId = parts[2];
    String serviceName = parts[3];
    return new ApplicationId(namespaceId, appId).service(serviceName);
  }

  private ServiceDiscoverable() {
    // private constructor
  }
}
