/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.meta;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.guava.ClassPath;
import io.cdap.cdap.common.internal.remote.MethodArgument;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that {@link RemotePrivilegesHandler} only deserializes the argument types it expects, and
 * that it rejects every other type named in a request without loading it.
 */
public class RemotePrivilegesHandlerTest {

  private static final Gson GSON = new Gson();

  private static final String ENTITY_ID_PACKAGE = EntityId.class.getPackage().getName();
  // The number of concrete EntityId classes in cdap-proto when this test was written. Guards
  // against a scan that silently finds nothing.
  private static final int MIN_ENTITY_ID_CLASSES = 28;

  private static final String ALICE_JSON = "{\"name\":\"alice\",\"type\":\"USER\"}";
  private static final String ADMINS_JSON = "{\"name\":\"admins\",\"type\":\"ROLE\"}";

  // Set by the static initializers of the canary classes below. The canaries are only ever named
  // through Class.getName(), which does not initialize a class, so a flag can only become true if
  // the handler resolved and initialized the class named in a request.
  private static final AtomicBoolean objectCanaryInitialized = new AtomicBoolean();
  private static final AtomicBoolean nullValueCanaryInitialized = new AtomicBoolean();

  private static final RecordingPermissionManager permissionManager =
      new RecordingPermissionManager();

  private static NettyHttpService httpService;
  private static URL baseUrl;

  @BeforeClass
  public static void setup() throws Exception {
    RemotePrivilegesHandler handler = new RemotePrivilegesHandler(permissionManager,
        new NoOpAccessController());
    httpService = NettyHttpService.builder("RemotePrivilegesHandlerTest")
        .setHttpHandlers(handler)
        .setExceptionHandler(new HttpExceptionHandler())
        .build();
    httpService.start();
    InetSocketAddress address = httpService.getBindAddress();
    baseUrl = new URL(String.format("http://%s:%d/v1/execute/", address.getHostName(),
        address.getPort()));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    httpService.stop();
  }

  @Before
  public void resetPermissionManager() {
    permissionManager.calls.clear();
  }

  @Test
  public void testEveryConcreteEntityIdClassIsAccepted() throws Exception {
    // Finds the id classes independently of the handler, so an id class added to cdap-proto
    // without updating the handler makes this test fail.
    ClassLoader classLoader = EntityId.class.getClassLoader();
    ClassPath classPath = ClassPath.from(
        EntityId.class.getProtectionDomain().getCodeSource().getLocation().toURI(), classLoader);
    List<Class<?>> idClasses = new ArrayList<>();
    for (ClassPath.ClassInfo classInfo : classPath.getAllClasses()) {
      if (!ENTITY_ID_PACKAGE.equals(classInfo.getPackageName())) {
        continue;
      }
      Class<?> cls = Class.forName(classInfo.getName(), false, classLoader);
      if (EntityId.class.isAssignableFrom(cls) && !Modifier.isAbstract(cls.getModifiers())) {
        idClasses.add(cls);
      }
    }
    Assert.assertTrue("Found only " + idClasses, idClasses.size() >= MIN_ENTITY_ID_CLASSES);

    AbstractRemoteSystemOpsHandler handler = new AbstractRemoteSystemOpsHandler();
    List<String> rejected = new ArrayList<>();
    for (Class<?> idClass : idClasses) {
      Iterator<MethodArgument> arguments = Collections.singletonList(
          new MethodArgument(idClass.getName(), new JsonObject())).iterator();
      try {
        Object entityId = handler.deserializeNext(arguments);
        // Built from an empty object, so only the class is checked; toString() would fail on it.
        if (entityId == null || entityId.getClass() != idClass) {
          rejected.add(idClass.getName() + " (got " + entityId + ")");
        }
      } catch (BadRequestException e) {
        rejected.add(idClass.getName());
      }
    }
    Assert.assertEquals("Entity id classes rejected by the handler", Collections.emptyList(),
        rejected);
  }

  @Test
  public void testPrincipalAndRoleAreAccepted() throws Exception {
    HttpResponse response = post("listPrivileges", argument(Principal.class.getName(), ALICE_JSON));

    Assert.assertEquals(response.getResponseBodyAsString(), 200, response.getResponseCode());
    Principal principal = permissionManager.onlyCall().principal;
    Assert.assertSame(Principal.class, principal.getClass());
    Assert.assertEquals(new Principal("alice", Principal.PrincipalType.USER), principal);

    permissionManager.calls.clear();
    response = post("listPrivileges", argument(Role.class.getName(), ADMINS_JSON));

    Assert.assertEquals(response.getResponseBodyAsString(), 200, response.getResponseCode());
    Principal role = permissionManager.onlyCall().principal;
    // Principal.equals also matches a Principal of type ROLE, so check the exact class too.
    Assert.assertSame(Role.class, role.getClass());
    Assert.assertEquals(new Role("admins"), role);
  }

  @Test
  public void testDisallowedTypesAreRejected() throws Exception {
    List<String> arguments = Arrays.asList(
        argument("io.cdap.cdap.nonexistent.ProbeType", ALICE_JSON),
        "{\"value\":" + ALICE_JSON + "}",
        argument(ObjectCanary.class.getName(), "{}"),
        argument(NullValueCanary.class.getName(), "null"));

    for (String argument : arguments) {
      permissionManager.calls.clear();

      HttpResponse response = post("listPrivileges", argument);

      Assert.assertEquals(argument + " " + response.getResponseBodyAsString(), 400,
          response.getResponseCode());
      Assert.assertTrue(argument, permissionManager.calls.isEmpty());
    }
    Assert.assertFalse(objectCanaryInitialized.get());
    Assert.assertFalse(nullValueCanaryInitialized.get());
  }

  @Test
  public void testGrantAcceptsEntityAndIgnoresPermissionsType() throws Exception {
    NamespaceId namespace = new NamespaceId("ns");

    // The permission set is deserialized with an explicit type, so the type name the client sends
    // for it (the runtime class of the set) is not checked.
    HttpResponse response = post("grant",
        argument(NamespaceId.class.getName(), GSON.toJson(namespace)),
        argument(Principal.class.getName(), ALICE_JSON),
        argument("java.util.HashSet", "[]"));

    Assert.assertEquals(response.getResponseBodyAsString(), 200, response.getResponseCode());
    Call call = permissionManager.onlyCall();
    Assert.assertEquals("grant", call.method);
    Assert.assertEquals(Authorizable.fromEntityId(namespace), call.authorizable);
    Assert.assertEquals(new Principal("alice", Principal.PrincipalType.USER), call.principal);
    Assert.assertEquals(Collections.emptySet(), call.permissions);
  }

  @Test
  public void testNullArgumentIsPassedThrough() throws Exception {
    HttpResponse response = post("listPrivileges", "null");

    Assert.assertEquals(response.getResponseBodyAsString(), 200, response.getResponseCode());
    Call call = permissionManager.onlyCall();
    Assert.assertEquals("listGrants", call.method);
    Assert.assertNull(call.principal);
  }

  /**
   * Returns one JSON-encoded {@link MethodArgument} with the given type and raw JSON value.
   */
  private static String argument(String type, String valueJson) {
    return "{\"type\":" + GSON.toJson(type) + ",\"value\":" + valueJson + "}";
  }

  private static HttpResponse post(String method, String... arguments) throws Exception {
    String body = "[" + String.join(",", arguments) + "]";
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, new URL(baseUrl, method))
        .withBody(body)
        .build();
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }

  /**
   * Canary whose initialization must never be triggered by naming it with a JSON object value.
   */
  private static final class ObjectCanary {
    static {
      objectCanaryInitialized.set(true);
    }
  }

  /**
   * Canary whose initialization must never be triggered by naming it with a JSON null value.
   */
  private static final class NullValueCanary {
    static {
      nullValueCanaryInitialized.set(true);
    }
  }

  /**
   * One call received by the {@link RecordingPermissionManager}.
   */
  private static final class Call {

    private final String method;
    @Nullable
    private final Authorizable authorizable;
    @Nullable
    private final Principal principal;
    @Nullable
    private final Set<? extends Permission> permissions;

    private Call(String method, @Nullable Authorizable authorizable,
        @Nullable Principal principal, @Nullable Set<? extends Permission> permissions) {
      this.method = method;
      this.authorizable = authorizable;
      this.principal = principal;
      this.permissions = permissions;
    }
  }

  /**
   * A {@link PermissionManager} that records every call and returns no grants.
   */
  private static final class RecordingPermissionManager implements PermissionManager {

    private final List<Call> calls = new CopyOnWriteArrayList<>();

    Call onlyCall() {
      Assert.assertEquals(1, calls.size());
      return calls.get(0);
    }

    @Override
    public void grant(Authorizable authorizable, Principal principal,
        Set<? extends Permission> permissions) {
      calls.add(new Call("grant", authorizable, principal, permissions));
    }

    @Override
    public void revoke(Authorizable authorizable, Principal principal,
        Set<? extends Permission> permissions) {
      calls.add(new Call("revoke", authorizable, principal, permissions));
    }

    @Override
    public void revoke(Authorizable authorizable) {
      calls.add(new Call("revokeAll", authorizable, null, null));
    }

    @Override
    public Set<GrantedPermission> listGrants(Principal principal) {
      calls.add(new Call("listGrants", null, principal, null));
      // A HashSet rather than Collections.emptySet(): Gson 2.3.1 cannot construct the latter's
      // private class reflectively on JDK 9 and later.
      return new HashSet<>();
    }
  }
}
