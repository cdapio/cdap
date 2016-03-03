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
package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.DefaultEndpointPluginContext;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactClassLoaderFactory;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.CloseableClassLoader;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import com.google.common.io.Closeables;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import javax.ws.rs.Path;

/**
 * To find, instantiate and invoke methods in plugin artifacts
 */
public class PluginService implements Closeable {
  private final ArtifactRepository artifactRepository;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final File stageDir;
  private final CConfiguration cConf;

  private CloseableClassLoader parentClassLoader;
  private PluginInstantiator pluginInstantiator;

  public PluginService(ArtifactRepository artifactRepository, File tmpDir, CConfiguration cConf) {
    this.artifactRepository = artifactRepository;
    this.stageDir = DirUtils.createTempDir(tmpDir);
    this.cConf = cConf;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, tmpDir);
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
  public PluginEndpoint getPluginEndpoint(Id.Namespace namespace,
                                          Id.Artifact artifactId, String pluginType,
                                          String pluginName, String methodName)
    throws IOException, NotFoundException, ClassNotFoundException {

    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    return getPluginEndpoint(namespace, artifactDetail, pluginType, pluginName,
                             pickParentArtifact(artifactDetail, artifactId), methodName);
  }

  private ArtifactDescriptor pickParentArtifact(ArtifactDetail artifactDetail, Id.Artifact artifact)
    throws ArtifactNotFoundException, IOException {

    // get parent artifacts
    Set<ArtifactRange> parentArtifactRanges = artifactDetail.getMeta().getUsableBy();
    if (parentArtifactRanges.isEmpty()) {
      throw new ArtifactNotFoundException(artifact);
    }

    // just pick the first parent artifact from the set.
    ArtifactRange parentArtifactRange = parentArtifactRanges.iterator().next();

    List<ArtifactDetail> artifactDetails = artifactRepository.getArtifacts(parentArtifactRange);
    if (artifactDetails.isEmpty()) {
      // should not happen
      throw new ArtifactNotFoundException(artifact);
    }

    // return the first one from the artifact details list.
    return artifactDetails.get(0).getDescriptor();
  }

  private PluginEndpoint getPluginEndpoint(Id.Namespace namespace, ArtifactDetail artifactDetail, String pluginType,
                                           String pluginName, ArtifactDescriptor parentArtifactDescriptor,
                                           String methodName)
    throws NotFoundException, IOException, ClassNotFoundException {

    Id.Artifact artifactId = Id.Artifact.from(namespace, artifactDetail.getDescriptor().getArtifactId());
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
    parentClassLoader = artifactClassLoaderFactory.createClassLoader(parentArtifactDescriptor.getLocation());
    pluginInstantiator = new PluginInstantiator(cConf, parentClassLoader, stageDir);

    // add plugin artifact
    pluginInstantiator.addArtifact(artifactDetail.getDescriptor().getLocation(), artifactId.toArtifactId());

    // we pass the parent artifact to endpoint plugin context,
    // as plugin method will use this context to load other plugins.
    DefaultEndpointPluginContext defaultEndpointPluginContext =
      new DefaultEndpointPluginContext(namespace.toEntityId(), artifactRepository, pluginInstantiator,
                                       Id.Artifact.from(namespace, parentArtifactDescriptor.getArtifactId()));

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

  @Override
  public void close() throws IOException {
    if (pluginInstantiator != null) {
      Closeables.closeQuietly(pluginInstantiator);
    }
    if (parentClassLoader != null) {
      Closeables.closeQuietly(parentClassLoader);
    }
    DirUtils.deleteDirectoryContents(stageDir);
  }
}
