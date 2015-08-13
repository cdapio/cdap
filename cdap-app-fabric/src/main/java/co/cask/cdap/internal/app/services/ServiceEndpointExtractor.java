/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.internal.lang.MethodVisitor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Extract the endpoints exposed by a {@link HttpServiceHandler}.
 */
public final class ServiceEndpointExtractor extends MethodVisitor {
  private final List<ServiceHttpEndpoint> endpoints;

  public ServiceEndpointExtractor(List<ServiceHttpEndpoint> endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType,
                    TypeToken<?> declareType, Method method) throws Exception {

    if (!Modifier.isPublic(method.getModifiers())) {
      return;
    }

    Path classPathAnnotation = inspectType.getRawType().getAnnotation(Path.class);
    Path methodPathAnnotation = method.getAnnotation(Path.class);

    if (methodPathAnnotation == null && classPathAnnotation == null) {
      return;
    }

    // Find one or more request type annotations present on the method.
    Set<Class<? extends Annotation>> acceptedMethodTypes = ImmutableSet.of(GET.class, POST.class, DELETE.class,
                                                                           PUT.class, OPTIONS.class, HEAD.class);

    Set<Class<? extends Annotation>> methodAnnotations = Sets.newHashSet();
    for (Annotation annotation : method.getAnnotations()) {
      Class<? extends Annotation> annotationClz = annotation.annotationType();
      if (acceptedMethodTypes.contains(annotationClz)) {
        methodAnnotations.add(annotationClz);
      }
    }

    for (Class<? extends Annotation> methodTypeClz : methodAnnotations) {
      String methodType  = methodTypeClz.getAnnotation(HttpMethod.class).value();
      String endpoint = "/";

      endpoint = classPathAnnotation == null ? endpoint : endpoint + classPathAnnotation.value();
      endpoint = methodPathAnnotation == null ? endpoint : endpoint + "/" + methodPathAnnotation.value();

      // Replace consecutive instances of / with a single instance.
      endpoint = endpoint.replaceAll("/+", "/");
      endpoints.add(new ServiceHttpEndpoint(methodType, endpoint));
    }
  }
}
