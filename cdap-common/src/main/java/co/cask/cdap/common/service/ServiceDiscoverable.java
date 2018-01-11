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

/**
 * Utility class to generate the service discoverable name (used for registering and discovering service endpoints in
 * ZooKeeper)
 */
public final class ServiceDiscoverable {

  public static String getName(ProgramId programId) {
    return getName(programId.getNamespace(), programId.getApplication(), programId.getProgram());
  }

  public static String getName(String namespaceId, String appId, String serviceId) {
    return String.format("%s.%s.%s.%s", ProgramType.SERVICE.name().toLowerCase(), namespaceId, appId, serviceId);
  }

  public static ProgramId getId(String name) {
    int firstIndex = name.indexOf('.');
    int secondIndex = name.indexOf('.', firstIndex + 1);
    int thirdIndex = name.indexOf('.', secondIndex + 1);
    String namespaceId = name.substring(firstIndex + 1, secondIndex);
    String appId = name.substring(secondIndex + 1, thirdIndex);
    String serviceName = name.substring(thirdIndex + 1);
    return new ApplicationId(namespaceId, appId).service(serviceName);
  }

  public static boolean isServiceDiscoverable(String discoverableName) {
    return discoverableName.startsWith(String.format("%s.", ProgramType.SERVICE.name().toLowerCase()));
  }

  private ServiceDiscoverable() {
    // private constructor
  }
}
