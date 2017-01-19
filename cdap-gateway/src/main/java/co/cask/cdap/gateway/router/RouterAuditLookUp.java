/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.logging.AuditLogContent;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.internal.asm.Classes;
import co.cask.http.HttpHandler;
import co.cask.http.PatternPathRouterWithGroups;
import com.google.common.base.Function;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Class to match the request path to the audit log content that needs to be logged.
 */
public final class RouterAuditLookUp {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAuditLookUp.class);
  private static final RouterAuditLookUp AUDIT_LOOK_UP = new RouterAuditLookUp();
  private static final int maxParts = 25;

  private final PatternPathRouterWithGroups<AuditLogContent> patternMatcher =
    PatternPathRouterWithGroups.create(maxParts);

  private RouterAuditLookUp() {
    createMatcher();
  }

  public static RouterAuditLookUp getAuditLookUp() {
    return AUDIT_LOOK_UP;
  }

  @Nullable
  public AuditLogContent getAuditLogContent(String path, HttpMethod httpMethod) throws Exception {
    List<PatternPathRouterWithGroups.RoutableDestination<AuditLogContent>> destinations =
      patternMatcher.getDestinations(path);
    for (PatternPathRouterWithGroups.RoutableDestination<AuditLogContent> entry : destinations) {
      AuditLogContent destination = entry.getDestination();
      if (destination.getHttpMethod().equals(httpMethod)) {
        return destination;
      }
    }
    return null;
  }

  private void createMatcher() {
    List<Class<?>> handlerClasses;
    try {
      handlerClasses = getAllHandlerClasses();
    } catch (IOException e) {
      LOG.error("Failed to get all handler classes for audit logging: {}", e.getCause());
      return;
    }

    int count = 0;
    for (Class<?> handlerClass : handlerClasses) {
      Path classPath = handlerClass.getAnnotation(Path.class);
      String classPathStr = classPath == null ? "" : classPath.value();
      for (Method method : handlerClass.getMethods()) {
        Path methodPath = method.getAnnotation(Path.class);
        AuditPolicy auditPolicy = method.getAnnotation(AuditPolicy.class);
        HttpMethod httpMethod = getHttpMethod(method);
        if (methodPath == null || auditPolicy == null || httpMethod == null) {
          continue;
        }

        String methodPathStr = methodPath.value();
        String completePath = classPathStr.endsWith("/") || methodPathStr.startsWith("/")
          ? classPathStr + methodPathStr : classPathStr + "/" + methodPathStr;
        List<AuditDetail> auditContents = Arrays.asList(auditPolicy.value());
        List<String> headerNames = new ArrayList<>();
        if (auditContents.contains(AuditDetail.HEADERS)) {
          Annotation[][] annotations = method.getParameterAnnotations();
          for (Annotation[] annotationArr : annotations) {
            if (annotationArr.length > 0) {
              for (Annotation annotation : annotationArr) {
                if (annotation instanceof HeaderParam) {
                  headerNames.add(((HeaderParam) annotation).value());
                }
              }
            }
          }
        }

        AuditLogContent auditLogContent = new AuditLogContent(httpMethod,
                                                              auditContents.contains(AuditDetail.REQUEST_BODY),
                                                              auditContents.contains(AuditDetail.RESPONSE_BODY),
                                                              headerNames);
        LOG.trace("Audit log lookup: bootstrapped with path: {}", completePath);
        patternMatcher.add(completePath, auditLogContent);
        count++;
      }
    }
    LOG.debug("Audit log lookup: bootstrapped with {} paths", count);
  }

  private HttpMethod getHttpMethod(Method method) {
    if (method.isAnnotationPresent(PUT.class)) {
      return HttpMethod.PUT;
    }
    if (method.isAnnotationPresent(POST.class)) {
      return HttpMethod.POST;
    }
    if (method.isAnnotationPresent(DELETE.class)) {
      return HttpMethod.DELETE;
    }
    return null;
  }

  private List<Class<?>> getAllHandlerClasses() throws IOException {
    ClassLoader cl = getClass().getClassLoader();
    Map<String, Boolean> cache = new HashMap<>();
    Function<String, URL> lookup = ClassLoaders.createClassResourceLookup(cl);
    ClassPath cp = ClassPath.from(cl);
    List<Class<?>> results = new ArrayList<>();
    for (ClassPath.ClassInfo info : cp.getAllClasses()) {
      if (!info.getPackageName().startsWith("co.cask.cdap")) {
        continue;
      }

      if (Classes.isSubTypeOf(info.getName(), HttpHandler.class.getName(), lookup, cache)) {
        results.add(info.load());
      }
    }
    return results;
  }
}
