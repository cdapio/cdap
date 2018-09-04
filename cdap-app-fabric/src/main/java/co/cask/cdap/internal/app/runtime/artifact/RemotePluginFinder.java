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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PluginClassDeserializer;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link PluginFinder} that use the artifact HTTP endpoints for finding plugins.
 */
public class RemotePluginFinder implements PluginFinder {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();
  private static final Type PLUGIN_INFO_LIST_TYPE = new TypeToken<List<PluginInfo>>() { }.getType();

  private final RemoteClient remoteClient;
  private final boolean authorizationEnabled;
  private final AuthenticationContext authenticationContext;
  private final LocationFactory locationFactory;
  private final RetryStrategy retryStrategy;

  @Inject
  RemotePluginFinder(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                     AuthenticationContext authenticationContext,
                     LocationFactory locationFactory) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         String.format("%s", Constants.Gateway.API_VERSION_3));
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.authenticationContext = authenticationContext;
    this.locationFactory = locationFactory;
    this.retryStrategy = RetryStrategies.limit(30, RetryStrategies.fixDelay(2, TimeUnit.SECONDS));
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId pluginNamespaceId,
                                                               ArtifactId parentArtifactId,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector)
    throws PluginNotExistsException {

    try {
      return Retries.callWithRetries(() -> {
        List<PluginInfo> infos = getPlugins(pluginNamespaceId, parentArtifactId, pluginType, pluginName);
        if (infos.isEmpty()) {
          throw new PluginNotExistsException(pluginNamespaceId, pluginType, pluginName);
        }

        SortedMap<co.cask.cdap.api.artifact.ArtifactId, PluginClass> plugins = new TreeMap<>();

        for (PluginInfo info : infos) {
          ArtifactSummary artifactSummary = info.getArtifact();
          co.cask.cdap.api.artifact.ArtifactId pluginArtifactId = new co.cask.cdap.api.artifact.ArtifactId(
            artifactSummary.getName(), new ArtifactVersion(artifactSummary.getVersion()), artifactSummary.getScope());
          PluginClass pluginClass = new PluginClass(info.getType(), info.getName(), info.getDescription(),
                                                    info.getClassName(), info.getConfigFieldName(),
                                                    info.getProperties(), info.getEndpoints());
          plugins.put(pluginArtifactId, pluginClass);
        }

        Map.Entry<co.cask.cdap.api.artifact.ArtifactId, PluginClass> selected = selector.select(plugins);
        if (selected == null) {
          throw new PluginNotExistsException(pluginNamespaceId, pluginType, pluginName);
        }

        Location artifactLocation = getArtifactLocation(Artifacts.toArtifactId(pluginNamespaceId, selected.getKey()));
        return Maps.immutableEntry(new ArtifactDescriptor(selected.getKey(), artifactLocation), selected.getValue());
      }, retryStrategy);
    } catch (PluginNotExistsException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a list of {@link PluginInfo} from the artifact extension endpoint.
   *
   * @param namespaceId namespace of the call happening in
   * @param parentArtifactId the parent artifact id
   * @param pluginType the plugin type to look for
   * @param pluginName the plugin name to look for
   * @return a list of {@link PluginInfo}
   * @throws IOException if it failed to get the information
   * @throws PluginNotExistsException if the given plugin type and name doesn't exist
   */
  private List<PluginInfo> getPlugins(NamespaceId namespaceId,
                                      ArtifactId parentArtifactId,
                                      String pluginType,
                                      String pluginName) throws IOException, PluginNotExistsException {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.GET,
        String.format("namespaces/%s/artifacts/%s/versions/%s/extensions/%s/plugins/%s?scope=%s&pluginScope=%s",
                      namespaceId.getNamespace(), parentArtifactId.getArtifact(),
                      parentArtifactId.getVersion(), pluginType, pluginName,
                      NamespaceId.SYSTEM.equals(parentArtifactId.getNamespaceId())
                        ? ArtifactScope.SYSTEM : ArtifactScope.USER,
                      NamespaceId.SYSTEM.equals(namespaceId.getNamespaceId())
                        ? ArtifactScope.SYSTEM : ArtifactScope.USER
                      ));
    // add header if auth is enabled
    if (authorizationEnabled) {
      requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }

    HttpResponse response = remoteClient.execute(requestBuilder.build());

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new PluginNotExistsException(namespaceId, pluginType, pluginName);
    }

    if (response.getResponseCode() != 200) {
      throw new IllegalArgumentException("Failure in getting plugin information with type " + pluginType + " and name "
                                           + pluginName + " that extends " + parentArtifactId
                                           + ". Reason is " + response.getResponseCode() + ": "
                                           + response.getResponseBodyAsString());
    }

    return GSON.fromJson(response.getResponseBodyAsString(), PLUGIN_INFO_LIST_TYPE);
  }


  /**
   * Retrieves the {@link Location} of a given artifact.
   */
  private Location getArtifactLocation(ArtifactId artifactId) throws IOException {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.GET, String.format("namespaces/%s/artifact-internals/artifacts/%s/versions/%s/location",
                                      artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion()));
    // add header if auth is enabled
    if (authorizationEnabled) {
      requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }

    HttpResponse response = remoteClient.execute(requestBuilder.build());

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException("Could not get artifact detail, endpoint not found");
    }
    if (response.getResponseCode() != 200) {
      throw new IOException("Exception while getting artifacts list: " + response.getResponseCode()
                              + ": " + response.getResponseBodyAsString());
    }

    String path = response.getResponseBodyAsString();
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    if (!location.exists()) {
      throw new IOException(String.format("Artifact Location does not exist %s for artifact %s version %s",
                                          path, artifactId.getArtifact(), artifactId.getVersion()));
    }
    return location;
  }
}
