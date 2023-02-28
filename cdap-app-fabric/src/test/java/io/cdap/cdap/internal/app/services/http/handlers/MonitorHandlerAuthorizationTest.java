/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.twill.MasterServiceManager;
import io.cdap.cdap.gateway.handlers.MonitorHandler;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.SystemServiceId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Monitor handler authorization tests.
 */
public class MonitorHandlerAuthorizationTest {
  private static final String SERVICE_NAME = "service";
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                       Principal.PrincipalType.USER);

  private static final Gson GSON = new Gson();

  private static MonitorHandler createMonitorHandler(Authorizable authorizable, List<Permission> requiredPermissions) {
    Map<String, MasterServiceManager> masterServiceManagerMap = new HashMap<>();
    MasterServiceManager mockMasterServiceManager = mock(MasterServiceManager.class);
    when(mockMasterServiceManager.getInstances()).thenReturn(1);
    when(mockMasterServiceManager.isServiceEnabled()).thenReturn(true);
    when(mockMasterServiceManager.getMinInstances()).thenReturn(1);
    when(mockMasterServiceManager.getMaxInstances()).thenReturn(1);
    when(mockMasterServiceManager.setInstances(any(Integer.class))).thenReturn(true);
    masterServiceManagerMap.put(SERVICE_NAME, mockMasterServiceManager);
    ServiceStore mockServiceStore = mock(ServiceStore.class);
    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(authorizable, MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(requiredPermissions)));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();
    DefaultContextAccessEnforcer contextAccessEnforcer = new DefaultContextAccessEnforcer(authenticationContext,
                                                                                          inMemoryAccessController);
    return new MonitorHandler(masterServiceManagerMap, mockServiceStore, contextAccessEnforcer);
  }

  @Test
  public void testGetSystemServiceLiveInfoAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(StandardPermission.GET));
    HttpRequest request = mock(HttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.getServiceLiveInfo(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.getServiceLiveInfo(request, responder, SERVICE_NAME);
  }

  @Test
  public void testSetServiceInstanceAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(StandardPermission.UPDATE));
    Instances instances = new Instances(1);
    DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
                                                                "system/services/service/instances",
                                                                Unpooled.copiedBuffer(GSON.toJson(instances),
                                                                                      StandardCharsets.UTF_8));
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.setServiceInstance(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.setServiceInstance(request, responder, SERVICE_NAME);
  }

  @Test
  public void testGetBootStatusAuthorization() throws Exception {
    InstanceId instanceId = InstanceId.SELF;
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(instanceId, EntityType.SYSTEM_SERVICE),
                                                  Arrays.asList(StandardPermission.LIST));
    HttpRequest request = mock(HttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.getBootStatus(request, responder);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.getBootStatus(request, responder);
  }

  @Test
  public void testGetServiceSpecAuthorization() throws Exception {
    InstanceId instanceId = InstanceId.SELF;
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(instanceId, EntityType.SYSTEM_SERVICE),
                                                  Arrays.asList(StandardPermission.LIST));
    HttpRequest request = mock(HttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.getServiceSpec(request, responder);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.getServiceSpec(request, responder);
  }

  @Test
  public void testRestartAllServiceInstancesAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(ApplicationPermission.EXECUTE));
    FullHttpRequest request = mock(FullHttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.restartAllServiceInstances(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.restartAllServiceInstances(request, responder, SERVICE_NAME);
  }

  @Test
  public void testRestartServiceInstanceAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(ApplicationPermission.EXECUTE));
    FullHttpRequest request = mock(FullHttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.restartServiceInstance(request, responder, SERVICE_NAME, 0);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.restartServiceInstance(request, responder, SERVICE_NAME, 0);
  }

  @Test
  public void testGetLatestRestartServiceInstanceStatusAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(StandardPermission.GET));
    HttpRequest request = mock(HttpRequest.class);
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.getLatestRestartServiceInstanceStatus(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.getLatestRestartServiceInstanceStatus(request, responder, SERVICE_NAME);
  }

  @Test
  public void testUpdateServiceLogLevelsAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(StandardPermission.UPDATE));
    Map<String, String> bodyArgs = new HashMap<>();
    DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
                                                                        "/system/services/service/loglevels",
                                                                        Unpooled.copiedBuffer(GSON.toJson(bodyArgs),
                                                                                              StandardCharsets.UTF_8));
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.updateServiceLogLevels(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.updateServiceLogLevels(request, responder, SERVICE_NAME);
  }

  @Test
  public void testResetServiceLogLevelsAuthorization() throws Exception {
    SystemServiceId systemServiceId = new SystemServiceId(SERVICE_NAME);
    MonitorHandler handler = createMonitorHandler(Authorizable.fromEntityId(systemServiceId),
                                                  Arrays.asList(StandardPermission.UPDATE));
    List<String> bodyArgs = new ArrayList<>();
    DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                                        "system/services/service/resetloglevels",
                                                                        Unpooled.copiedBuffer(GSON.toJson(bodyArgs),
                                                                                              StandardCharsets.UTF_8));
    HttpResponder responder = mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.resetServiceLogLevels(request, responder, SERVICE_NAME);
    } catch (UnauthorizedException e) {
      // expected
    }

    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    handler.resetServiceLogLevels(request, responder, SERVICE_NAME);
  }

}
