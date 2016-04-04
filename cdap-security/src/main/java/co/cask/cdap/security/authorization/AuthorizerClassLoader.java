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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.security.spi.authorization.Authorizer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * {@link DirectoryClassLoader} for {@link Authorizer} extensions.
 */
public class AuthorizerClassLoader extends DirectoryClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizerClassLoader.class);

  @VisibleForTesting
  static ClassLoader createParent() {
    ClassLoader baseClassLoader = AuthorizerClassLoader.class.getClassLoader();
    Set<String> authorizerResources;
    try {
      authorizerResources = ClassPathResources.getResourcesWithDependencies(baseClassLoader, Authorizer.class);
    } catch (IOException e) {
      LOG.error("Failed to determine resources for authorizer class loader.", e);
      authorizerResources = ImmutableSet.of();
    }

    final Set<String> finalAuthorizerResources = authorizerResources;
    return new FilterClassLoader(baseClassLoader, new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return finalAuthorizerResources.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return true;
      }
    });
  }

  AuthorizerClassLoader(File unpackedJarDir) {
    super(unpackedJarDir, createParent(), "lib");
  }
}
