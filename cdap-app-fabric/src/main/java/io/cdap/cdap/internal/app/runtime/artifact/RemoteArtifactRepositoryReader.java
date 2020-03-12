package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RemoteArtifactRepositoryReader implements ArtifactRepositoryReader {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteArtifactRepositoryReader.class);
  public static final String LOCATION_FACTORY = "RemoteArtifactRepositoryReader_LocationFactory";
  public static final String DISCOVERY_SERVICE_CLIENT = "RemoteArtifactRepositoryReader_DiscoveryServiceClient";

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_DETAIL_TYPE = new TypeToken<ArtifactDetail>() { }.getType();
  private static final Type PLUGIN_INFO_LIST_TYPE = new TypeToken<List<PluginInfo>>() { }.getType();

  private final RemoteClient remoteClient;
  private final LocationFactory locationFactory;

  @Inject
  public RemoteArtifactRepositoryReader(@Named(DISCOVERY_SERVICE_CLIENT) DiscoveryServiceClient discoveryClient,
                                        @Named(LOCATION_FACTORY)LocationFactory locationFactory) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
    this.locationFactory = locationFactory;
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    HttpResponse httpResponse;
    String url = "namespaces/" + artifactId.getNamespace().getId() + "/artifacts/" + artifactId.getName() + "/versions/" + artifactId.getVersion();
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());
    ArtifactDetail detail = GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_DETAIL_TYPE);
    String path = detail.getDescriptor().getLocationURI().getPath();
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    return new ArtifactDetail(new ArtifactDescriptor(detail.getDescriptor().getArtifactId(), location),
                              detail.getMeta());
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName, PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() start");
    try {
        List<PluginInfo> infos = getPlugins(namespace, artifactId, pluginType, pluginName);
        LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() getPlugins done: size=" + infos.size());
        if (infos.isEmpty()) {
          LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() returned new PluginNotExistsException");
          throw new PluginNotExistsException(namespace, pluginType, pluginName);
        }

        SortedMap<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> plugins = new TreeMap<>();

        for (PluginInfo info : infos) {
          ArtifactSummary artifactSummary = info.getArtifact();
          io.cdap.cdap.api.artifact.ArtifactId pluginArtifactId = new io.cdap.cdap.api.artifact.ArtifactId(
            artifactSummary.getName(), new ArtifactVersion(artifactSummary.getVersion()), artifactSummary.getScope());
          PluginClass pluginClass = new PluginClass(info.getType(), info.getName(), info.getDescription(),
                                                    info.getClassName(), info.getConfigFieldName(),
                                                    info.getProperties());
          plugins.put(pluginArtifactId, pluginClass);
        }

        Map.Entry<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> selected = selector.select(plugins);
        if (selected == null) {
          LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() returned new PluginNotExistsException");
          throw new PluginNotExistsException(namespace, pluginType, pluginName);
        }

        Location artifactLocation = getArtifactLocation(Artifacts.toArtifactId(namespace, selected.getKey()));
        LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() returned something");
        return Maps.immutableEntry(new ArtifactDescriptor(selected.getKey(), artifactLocation), selected.getValue());
    } catch (PluginNotExistsException e) {
      LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() plugin not exists exception " + e);
      throw e;
    } catch (ArtifactNotFoundException e) {
      LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() artifact not found exception " + e);
      throw new PluginNotExistsException(namespace, pluginType, pluginName);
    } catch (Exception e) {
      LOG.debug("wyzhang: RemoteArtifactRepositoryReader::findPlugin() exception " + e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Retrieves the {@link Location} of a given artifact.
   */
  private Location getArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getArtifactLocation() start");
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/location",
                               artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion());
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getArtifactLocation() url = " + url);
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse response = remoteClient.execute(requestBuilder.build());
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getArtifactLocation() resp = " + response.getResponseBodyAsString());
    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new ArtifactNotFoundException(artifactId);
    }
    if (response.getResponseCode() != 200) {
      throw new IOException("Exception while getting artifacts list: " + response.getResponseCode()
                              + ": " + response.getResponseBodyAsString());
    }

    String path = response.getResponseBodyAsString();
        Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getArtifactLocation() location = " + location.toURI());
    if (!location.exists()) {
      throw new IOException(String.format("Artifact Location does not exist %s for artifact %s version %s",
                                          path, artifactId.getArtifact(), artifactId.getVersion()));
    }
    return location;
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
                                      Id.Artifact parentArtifactId,
                                      String pluginType,
                                      String pluginName) throws IOException, PluginNotExistsException {
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings start");
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings namespace id = " + namespaceId.toString());
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings parent artifact " + parentArtifactId.getNamespace() + " " + parentArtifactId.getName());
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings plugintype = " + pluginType);
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings pluginName = " + pluginName);
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/extensions/%s/plugins/%s?scope=%s",
                      namespaceId.getNamespace(), parentArtifactId.getName(),
                      parentArtifactId.getVersion(), pluginType, pluginName,
                      Id.Namespace.SYSTEM.getId().equals(parentArtifactId.getNamespace().getId())
                        ?  ArtifactScope.SYSTEM : ArtifactScope.USER);
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings url = " + url);
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(HttpMethod.GET, url);
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings execute");
    HttpResponse response = remoteClient.execute(requestBuilder.build());

    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings response body = " + response.getResponseBodyAsString());
    LOG.debug("wyzhang: RemoteArtifactRepositoryReader::getPluings response code = " + response.getResponseCode());
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


  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }

}
