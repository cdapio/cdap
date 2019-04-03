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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class to generate the service discoverable name (used for registering and discovering service endpoints in
 * ZooKeeper)
 */
public final class ServiceDiscoverable {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceDiscoverable.class);

  private static final Set<ProgramType> USER_SERVICE_TYPES = Collections.unmodifiableSet(EnumSet.of(ProgramType.SERVICE,
                                                                                                    ProgramType.SPARK));

  public static String getName(ProgramId programId) {
    return getName(programId.getNamespace(), programId.getApplication(), programId.getType(), programId.getProgram());
  }

  public static String getName(String namespaceId, String appId, ProgramType programType, String programName) {
    if (!USER_SERVICE_TYPES.contains(programType)) {
      throw new IllegalArgumentException("Program type should be one of " + USER_SERVICE_TYPES);
    }
    return String.format("%s.%s.%s.%s", programType.name().toLowerCase(), namespaceId, appId, programName);
  }

  public static ProgramId getId(String name) {
    int firstIndex = name.indexOf('.');
    int secondIndex = name.indexOf('.', firstIndex + 1);
    int thirdIndex = name.indexOf('.', secondIndex + 1);
    String programType = name.substring(0, firstIndex);
    String namespaceId = name.substring(firstIndex + 1, secondIndex);
    String appId = name.substring(secondIndex + 1, thirdIndex);
    String programName = name.substring(thirdIndex + 1);

    return new ProgramId(namespaceId, appId, programType, programName);
  }

  public static boolean isUserService(String discoverableName) {
    for (ProgramType type : USER_SERVICE_TYPES) {
      if (discoverableName.startsWith(type.name().toLowerCase() + ".")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the set of {@link ProgramType} that can have user service handlers.
   */
  public static Set<ProgramType> getUserServiceTypes() {
    return USER_SERVICE_TYPES;
  }

  /**
   * Creates a base {@link URL} for calling user service based on the given {@link Discoverable}.
   *
   * @return a {@link URL} or {@code null} if the discoverable is {@code null} or failed to construct a URL.
   */
  @Nullable
  public static URL createServiceBaseURL(@Nullable Discoverable discoverable, ProgramId programId) {
    if (discoverable == null) {
      return null;
    }

    InetSocketAddress address = discoverable.getSocketAddress();
    boolean ssl = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload());
    return createServiceBaseURL(address.getHostName(), address.getPort(), ssl, programId);
  }

  /**
   * Creates a base {@link URL} for calling user service.
   *
   * @param host hostname of the router
   * @param port port of the router
   * @param ssl {@code true} true to use SSL, {@code false} otherwise.
   * @param programId the program id of the user service.
   * @return a {@link URL} that serves as the base URL
   */
  public static URL createServiceBaseURL(String host, int port, boolean ssl, ProgramId programId) {
    String scheme = ssl ? Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;
    String path = String.format("%s%s:%d%s/namespaces/%s/apps/%s/%s/%s/methods/", scheme,
                                host, port,
                                Constants.Gateway.API_VERSION_3, programId.getNamespace(),
                                programId.getApplication(), programId.getType().getCategoryName(),
                                programId.getProgram());
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      // This shouldn't happen
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }

  private ServiceDiscoverable() {
    // private constructor
  }
}
