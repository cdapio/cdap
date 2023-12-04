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

package io.cdap.cdap.master.environment;

import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.proto.ProgramType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.utils.Instances;

/** Default implementation of {@link MasterEnvironmentRunnableContext}. */
public class DefaultMasterEnvironmentRunnableContext implements MasterEnvironmentRunnableContext {

  private final LocationFactory locationFactory;
  private final RemoteClient remoteClient;
  private final CConfiguration cConf;
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;

  private ClassLoader extensionCombinedClassLoader;

  public DefaultMasterEnvironmentRunnableContext(
      LocationFactory locationFactory,
      RemoteClientFactory remoteClientFactory,
      CConfiguration cConf) {
    this.locationFactory = locationFactory;
    this.remoteClient =
        remoteClientFactory.createRemoteClient(
            Constants.Service.APP_FABRIC_HTTP, new DefaultHttpRequestConfig(false), "");
    this.cConf = cConf;
    this.programRuntimeProviderLoader = new ProgramRuntimeProviderLoader(cConf);
    this.extensionCombinedClassLoader = null;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  /** Opens a {@link HttpURLConnection} for the given resource path. */
  @Override
  public HttpURLConnection openHttpURLConnection(String resource) throws IOException {
    return remoteClient.openConnection(resource);
  }

  @Override
  public TwillRunnable instantiateTwillRunnable(String className) {
    Class<?> runnableClass;
    try {
      runnableClass = getClass().getClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      // Try loading the class from the runtime extensions.
      if (extensionCombinedClassLoader == null) {
        Map<ProgramType, ProgramRuntimeProvider> classLoaderProviderMap =
            programRuntimeProviderLoader.getAll();
        extensionCombinedClassLoader =
            new CombineClassLoader(
                getClass().getClassLoader(),
                classLoaderProviderMap.entrySet().stream()
                    .map(entry -> entry.getValue().getRuntimeClassLoader(entry.getKey(), cConf))
                    .collect(Collectors.toList()));
      }
      try {
        runnableClass = extensionCombinedClassLoader.loadClass(className);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(
            String.format(
                "Failed to load twill runnable class from runtime extension '%s'", className),
            cnfe);
      }
    }
    if (!TwillRunnable.class.isAssignableFrom(runnableClass)) {
      throw new IllegalArgumentException(
          "Class " + runnableClass + " is not an instance of " + TwillRunnable.class);
    }
    return (TwillRunnable) Instances.newInstance(runnableClass);
  }
}
