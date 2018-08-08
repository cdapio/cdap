/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.internal.asm.Classes;
import co.cask.http.HttpHandler;
import com.google.common.base.Function;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;

public abstract class HandlerInspector {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAuditLookUp.class);


  List<ClassPath.ClassInfo> getAllHandlerClasses() throws IOException {
    ClassLoader cl = getClass().getClassLoader();
    Map<String, Boolean> cache = new HashMap<>();
    Function<String, URL> lookup = ClassLoaders.createClassResourceLookup(cl);
    ClassPath cp = ClassPath.from(cl);
    List<ClassPath.ClassInfo> results = new ArrayList<>();
    for (ClassPath.ClassInfo info : cp.getAllClasses()) {
      if (!info.getPackageName().startsWith("co.cask.cdap")) {
        continue;
      }
      if (Classes.isSubTypeOf(info.getName(), HttpHandler.class.getName(), lookup, cache)) {
        results.add(info);
      }
    }
    return results;
  }

  HttpMethod getHttpMethod(java.lang.reflect.Method method) {
    if (method.isAnnotationPresent(PUT.class)) {
      return HttpMethod.PUT;
    }
    if (method.isAnnotationPresent(POST.class)) {
      return HttpMethod.POST;
    }
    if (method.isAnnotationPresent(DELETE.class)) {
      return HttpMethod.DELETE;
    }
    if (method.isAnnotationPresent(GET.class)) {
      return HttpMethod.GET;
    }
    return null;
  }

  boolean isTestClass(ClassPath.ClassInfo classInfo) {
    URL url = classInfo.url();
    return url != null && url.getPath().contains("target/test-classes");
  }
}
