/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.DefaultEndpointPluginContext;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.Path;

/**
 * To find, instantiate and invoke methods in plugin artifacts
 */
@Singleton
public class PluginService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(PluginService.class);

  private final ArtifactRepository artifactRepository;
  private final File tmpDir;
  private final CConfiguration cConf;
  private final LoadingCache<ArtifactDescriptor, Instantiators> instantiators;
  private final Impersonator impersonator;

  private File stageDir;

  @Inject
  public PluginService(ArtifactRepository artifactRepository, CConfiguration cConf, Impersonator impersonator) {
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.cConf = cConf;
    this.instantiators = CacheBuilder.newBuilder()
      .removalListener(new InstantiatorsRemovalListener())
      .maximumWeight(100)
      .weigher(new Weigher<ArtifactDescriptor, Instantiators>() {
        @Override
        public int weigh(ArtifactDescriptor key, Instantiators value) {
          return value.size();
        }
      })
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new InstantiatorsCacheLoader());
    this.impersonator = impersonator;
  }

  /**
   * Given plugin artifact, find the parent artifact of plugin, after creating classloader with parent artifact
   * and plugin, we invoke the plugin method in the plugin identified by type and name and return the response.
   * @param namespace namespace
   * @param artifactId plugin artifact id
   * @param pluginType type of the plugin
   * @param pluginName name of the plugin
   * @param methodName name of the method
   * @return {@link PluginEndpoint}
   * @throws IOException
   * @throws NotFoundException
   * @throws ClassNotFoundException
   */
  public PluginEndpoint getPluginEndpoint(NamespaceId namespace,
                                          Id.Artifact artifactId, String pluginType,
                                          String pluginName, String methodName)
    throws Exception {
    // should not happen
    if (!isRunning()) {
      throw new ServiceUnavailableException("Plugin Service is not running currently");
    }

    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    return getPluginEndpoint(namespace, artifactDetail, pluginType, pluginName,
                             getParentArtifactDescriptor(artifactDetail, artifactId),
                             artifactDetail.getMeta().getUsableBy(), methodName);
  }

  private ArtifactDescriptor getParentArtifactDescriptor(ArtifactDetail artifactDetail, Id.Artifact artifact)
    throws Exception {
    // get parent artifact range
    Set<ArtifactRange> parentArtifactRanges = artifactDetail.getMeta().getUsableBy();
    if (parentArtifactRanges.isEmpty()) {
      throw new ArtifactNotFoundException(artifact.toEntityId());
    }
    List<ArtifactDetail> artifactDetails = artifactRepository.getArtifactDetails(parentArtifactRanges.iterator().next(),
                                                                                 Integer.MAX_VALUE,
                                                                                 ArtifactSortOrder.UNORDERED);
    return artifactDetails.iterator().next().getDescriptor();
  }

  @Override
  protected void startUp() {
    stageDir = DirUtils.createTempDir(tmpDir);
  }

  @Override
  protected void shutDown() {
    instantiators.invalidateAll();
    try {
      DirUtils.deleteDirectoryContents(stageDir);
    } catch (IOException e) {
      LOG.error("Error while deleting directory in PluginService", e);
    }
  }

  /**
   * A RemovalListener for closing classloader and plugin instantiators.
   */
  private static final class InstantiatorsRemovalListener implements
    RemovalListener<ArtifactDescriptor, Instantiators> {

    @Override
    public void onRemoval(RemovalNotification<ArtifactDescriptor, Instantiators> notification) {
      Closeables.closeQuietly(notification.getValue());
    }
  }

  private class Instantiators implements Closeable {
    private final CloseableClassLoader parentClassLoader;
    private final Map<ArtifactDescriptor, InstantiatorInfo> instantiatorInfoMap;
    private final File pluginDir;

    private Instantiators(ArtifactDescriptor parentArtifactDescriptor) throws IOException {
      // todo : shouldn't pass null, should use ArtifactId instead of ArtifactDescriptor so we have namespace.
      this.parentClassLoader =
        artifactRepository.createArtifactClassLoader(
          // todo : should not pass null, (Temporary)
          // change Instantiators to accept ArtifactId instead of ArtifactDescriptor
          parentArtifactDescriptor.getLocation(), new EntityImpersonator(null, impersonator));
      this.instantiatorInfoMap = new ConcurrentHashMap<>();
      this.pluginDir = DirUtils.createTempDir(stageDir);
    }

    private boolean hasArtifactChanged(ArtifactDescriptor artifactDescriptor) {
      if (instantiatorInfoMap.containsKey(artifactDescriptor) &&
        !instantiatorInfoMap.get(artifactDescriptor).getArtifactLocation().equals(artifactDescriptor.getLocation())) {
        return true;
      }
      return false;
    }

    private void addInstantiatorAndAddArtifact(ArtifactDetail artifactDetail,
                                               ArtifactId artifactId) throws IOException {
      PluginInstantiator instantiator = new PluginInstantiator(cConf, parentClassLoader, pluginDir);
      instantiatorInfoMap.put(artifactDetail.getDescriptor(),
                              new InstantiatorInfo(artifactDetail.getDescriptor().getLocation(), instantiator));
      instantiator.addArtifact(artifactDetail.getDescriptor().getLocation(), artifactId);
    }

    private PluginInstantiator getPluginInstantiator(ArtifactDetail artifactDetail,
                                                     ArtifactId artifactId) throws IOException {
      if (!instantiatorInfoMap.containsKey(artifactDetail.getDescriptor())) {
        addInstantiatorAndAddArtifact(artifactDetail, artifactId);
      } else if (hasArtifactChanged(artifactDetail.getDescriptor())) {
        instantiatorInfoMap.remove(artifactDetail.getDescriptor());
        addInstantiatorAndAddArtifact(artifactDetail, artifactId);
      }
      return instantiatorInfoMap.get(artifactDetail.getDescriptor()).getPluginInstantiator();
    }

    private int size() {
      return instantiatorInfoMap.size();
    }

    @Override
    public void close() throws IOException {
      for (InstantiatorInfo instantiatorInfo : instantiatorInfoMap.values()) {
        Closeables.closeQuietly(instantiatorInfo.getPluginInstantiator());
      }
      Closeables.closeQuietly(parentClassLoader);
      DirUtils.deleteDirectoryContents(pluginDir);
    }
  }

  private class InstantiatorInfo {
    private final Location artifactLocation;
    private final PluginInstantiator pluginInstantiator;

    private InstantiatorInfo(Location artifactLocation, PluginInstantiator pluginInstantiator) {
      this.artifactLocation = artifactLocation;
      this.pluginInstantiator = pluginInstantiator;
    }

    private Location getArtifactLocation() {
      return artifactLocation;
    }

    private PluginInstantiator getPluginInstantiator() {
      return pluginInstantiator;
    }
  }

  /**
   * A CacheLoader for creating Instantiators.
   */
  private final class InstantiatorsCacheLoader extends CacheLoader<ArtifactDescriptor, Instantiators> {

    @Override
    public Instantiators load(ArtifactDescriptor parentArtifactDescriptor) throws Exception {
      return new Instantiators(parentArtifactDescriptor);
    }
  }

  private PluginEndpoint getPluginEndpoint(NamespaceId namespace, ArtifactDetail artifactDetail, String pluginType,
                                           String pluginName, ArtifactDescriptor parentArtifactDescriptor,
                                           Set<ArtifactRange> parentArtifactRanges,
                                           String methodName)
    throws NotFoundException, IOException, ClassNotFoundException {
    Id.Artifact artifactId = Id.Artifact.from(namespace.toId(), artifactDetail.getDescriptor().getArtifactId());
    Set<PluginClass> pluginClasses = artifactDetail.getMeta().getClasses().getPlugins();
    PluginClass pluginClass = null;

    for (PluginClass plugin : pluginClasses) {
      if (plugin.getName().equals(pluginName) && plugin.getType().equals(pluginType)) {
        // plugin type and name matched, next check for endpoint method presence
        if (plugin.getEndpoints() == null || !plugin.getEndpoints().contains(methodName)) {
          throw new NotFoundException(
            String.format("Plugin with type: %s name: %s found, " +
                            "but Endpoint %s was not found", pluginType, pluginName, methodName));
        }
        pluginClass = plugin;
      }
    }

    if (pluginClass == null) {
      throw new NotFoundException(
        String.format("No Plugin with type : %s, name: %s was found", pluginType, pluginName));
    }

    // initialize parent classloader and plugin instantiator
    Instantiators instantiators = this.instantiators.getUnchecked(parentArtifactDescriptor);
    PluginInstantiator pluginInstantiator = instantiators.getPluginInstantiator(artifactDetail,
                                                                                artifactId.toArtifactId());

    // we pass the parent artifact to endpoint plugin context,
    // as plugin method will use this context to load other plugins.
    DefaultEndpointPluginContext defaultEndpointPluginContext =
      new DefaultEndpointPluginContext(namespace, artifactRepository, pluginInstantiator, parentArtifactRanges);

    return getPluginEndpoint(pluginInstantiator, artifactId,
                             pluginClass, methodName, defaultEndpointPluginContext);
  }

  /**
   * load and instantiate the plugin and return {@link PluginEndpoint}
   * which can be used to invoke plugin method with request object.
   *
   * @param pluginInstantiator to instantiate plugin instances.
   * @param artifact artifact of the plugin
   * @param pluginClass class having the plugin method to invoke
   * @param endpointName name of the endpoint to invoke
   * @param endpointPluginContext endpoint plugin context that can optionally be passed to the method
   * @throws IOException if there was an exception getting the classloader
   * @throws ClassNotFoundException if plugin class cannot be loaded
   * @throws NotFoundException Not Found exception thrown if no matching plugin found.
   */
  private PluginEndpoint getPluginEndpoint(final PluginInstantiator pluginInstantiator, Id.Artifact artifact,
                                           PluginClass pluginClass, String endpointName,
                                           final DefaultEndpointPluginContext endpointPluginContext)
    throws IOException, ClassNotFoundException, NotFoundException {

    ClassLoader pluginClassLoader = pluginInstantiator.getArtifactClassLoader(artifact.toArtifactId());
    Class pluginClassLoaded = pluginClassLoader.loadClass(pluginClass.getClassName());

    final Object pluginInstance = pluginInstantiator.newInstanceWithoutConfig(artifact.toArtifactId(), pluginClass);

    Method[] methods = pluginClassLoaded.getMethods();
    for (final Method method : methods) {
      Path pathAnnotation = method.getAnnotation(Path.class);
      // method should have path annotation else continue
      if (pathAnnotation == null || !pathAnnotation.value().equals(endpointName)) {
        continue;
      }

      return new PluginEndpoint() {
        @Override
        public java.lang.reflect.Type getMethodParameterType() throws IllegalArgumentException {
          if (method.getParameterTypes().length == 0) {
            // should not happen, checks should have happened during deploy artifact.
            throw new IllegalArgumentException("No Method parameter type found");
          }
          return method.getGenericParameterTypes()[0];
        }

        @Override
        public java.lang.reflect.Type getResultType() {
          return method.getGenericReturnType();
        }

        @Override
        public Object invoke(Object request) throws IOException, ClassNotFoundException,
          InvocationTargetException, IllegalAccessException, IllegalArgumentException {

          if (method.getParameterTypes().length == 2 &&
            EndpointPluginContext.class.isAssignableFrom(method.getParameterTypes()[1])) {
            return method.invoke(pluginInstance, request, endpointPluginContext);
          } else if (method.getParameterTypes().length == 1) {
            return method.invoke(pluginInstance, request);
          } else {
            throw new IllegalArgumentException(
              String.format("Only method with 1 parameter and optional EndpointPluginContext as 2nd parameter is " +
                              "allowed in Plugin endpoint method, Found %s parameters",
                            method.getParameterTypes().length));
          }
        }
      };
    };

    // cannot find the endpoint in plugin method. should not happen as this is checked earlier.
    throw new NotFoundException("Could not find the plugin method with the requested method endpoint {}", endpointName);
  }
}
