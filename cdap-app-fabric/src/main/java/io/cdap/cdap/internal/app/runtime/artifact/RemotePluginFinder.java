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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private static final Gson GSON = new Gson();
  private static final Type PLUGIN_INFO_LIST_TYPE = new TypeToken<List<PluginInfo>>() {
  }.getType();

  private final RemoteClient remoteClient;
  private final RemoteClient remoteClientInternal;
  private final LocationFactory locationFactory;
  private final RetryStrategy retryStrategy;

  @Inject
  public RemotePluginFinder(LocationFactory locationFactory, RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false),
      String.format("%s", Constants.Gateway.API_VERSION_3));
    this.remoteClientInternal = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false),
      String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
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

        SortedMap<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> plugins = new TreeMap<>();

        for (PluginInfo info : infos) {
          ArtifactSummary artifactSummary = info.getArtifact();
          io.cdap.cdap.api.artifact.ArtifactId pluginArtifactId = new io.cdap.cdap.api.artifact.ArtifactId(
            artifactSummary.getName(), new ArtifactVersion(artifactSummary.getVersion()), artifactSummary.getScope());
          PluginClass pluginClass =
            PluginClass.builder().setName(info.getName()).setType(info.getType())
              .setDescription(info.getDescription())
              .setClassName(info.getClassName())
              .setRuntimeClassNames(info.getRuntimeClassNames())
              .setProperties(info.getProperties())
              .setConfigFieldName(info.getConfigFieldName()).build();
          plugins.put(pluginArtifactId, pluginClass);
        }

        Map.Entry<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> selected = selector.select(plugins);
        if (selected == null) {
          throw new PluginNotExistsException(pluginNamespaceId, pluginType, pluginName);
        }

        Location artifactLocation = getArtifactLocation(Artifacts.toProtoArtifactId(pluginNamespaceId,
                                                                                    selected.getKey()));
        return Maps.immutableEntry(new ArtifactDescriptor(pluginNamespaceId.getEntityName(),
                                                          selected.getKey(), artifactLocation), selected.getValue());
      }, retryStrategy);
    } catch (PluginNotExistsException e) {
      throw e;
    } catch (ArtifactNotFoundException e) {
      throw new PluginNotExistsException(pluginNamespaceId, pluginType, pluginName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a list of {@link PluginInfo} from the artifact extension endpoint.
   *
   * @param namespaceId      namespace of the call happening in
   * @param parentArtifactId the parent artifact id
   * @param pluginType       the plugin type to look for
   * @param pluginName       the plugin name to look for
   * @return a list of {@link PluginInfo}
   * @throws IOException              if it failed to get the information
   * @throws PluginNotExistsException if the given plugin type and name doesn't exist
   */
  private List<PluginInfo> getPlugins(NamespaceId namespaceId,
                                      ArtifactId parentArtifactId,
                                      String pluginType,
                                      String pluginName)
    throws IOException, PluginNotExistsException, UnauthorizedException {
    // replace the space in the name
    // TODO: CDAP-18375 improve url encoding the our remote call
    pluginName = pluginName.replace(" ", "%20");
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
  protected Location getArtifactLocation(ArtifactId artifactId)
    throws IOException, ArtifactNotFoundException, UnauthorizedException {
    HttpRequest.Builder requestBuilder =
      remoteClientInternal.requestBuilder(
        HttpMethod.GET, String.format("namespaces/%s/artifacts/%s/versions/%s/location",
                                      artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion()));

    HttpResponse response = remoteClientInternal.execute(requestBuilder.build());

    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new ArtifactNotFoundException(artifactId);
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
