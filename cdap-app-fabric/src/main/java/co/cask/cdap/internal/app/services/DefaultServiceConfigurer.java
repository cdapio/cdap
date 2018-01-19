/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceConfigurer;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.internal.app.AbstractConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link ServiceConfigurer}.
 */
public class DefaultServiceConfigurer extends AbstractConfigurer implements ServiceConfigurer {

  private final String className;
  private final Id.Artifact artifactId;
  private final ArtifactRepository artifactRepository;
  private final PluginInstantiator pluginInstantiator;

  private String name;
  private String description;
  private List<HttpServiceHandler> handlers;
  private Resources resources;
  private int instances;

  /**
   * Create an instance of {@link DefaultServiceConfigurer}
   */
  public DefaultServiceConfigurer(Service service, Id.Namespace namespace, Id.Artifact artifactId,
                                  ArtifactRepository artifactRepository, PluginInstantiator pluginInstantiator) {
    super(namespace, artifactId, artifactRepository, pluginInstantiator);
    this.className = service.getClass().getName();
    this.name = service.getClass().getSimpleName();
    this.description = "";
    this.handlers = Lists.newArrayList();
    this.resources = new Resources();
    this.instances = 1;
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addHandlers(Iterable<? extends HttpServiceHandler> serviceHandlers) {
    Iterables.addAll(handlers, serviceHandlers);
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instances must be > 0.");
    this.instances = instances;
  }

  @Override
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources cannot be null.");
    this.resources = resources;
  }

  public ServiceSpecification createSpecification() {
    Map<String, HttpServiceHandlerSpecification> handleSpecs = createHandlerSpecs(handlers);
    return new ServiceSpecification(className, name, description, handleSpecs, resources, instances);
  }

  /**
   * Constructs HttpServiceSpecifications for each of the handlers in the {@param handlers} list.
   * Also performs verifications on these handlers (that a NettyHttpService can be constructed from them).
   */
  private Map<String, HttpServiceHandlerSpecification> createHandlerSpecs(List<? extends HttpServiceHandler> handlers) {
    verifyHandlers(handlers);
    Map<String, HttpServiceHandlerSpecification> handleSpecs = Maps.newHashMap();
    for (HttpServiceHandler handler : handlers) {
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(
        handler, deployNamespace, artifactId, artifactRepository, pluginInstantiator);
      handler.configure(configurer);
      HttpServiceHandlerSpecification spec = configurer.createSpecification();
      Preconditions.checkArgument(!handleSpecs.containsKey(spec.getName()),
                                  "Handler with name %s already existed.", spec.getName());
      handleSpecs.put(spec.getName(), spec);
      addStreams(configurer.getStreams());
      addDatasetModules(configurer.getDatasetModules());
      addDatasetSpecs(configurer.getDatasetSpecs());
      addPlugins(configurer.getPlugins());
    }
    return handleSpecs;
  }

  private void verifyHandlers(List<? extends HttpServiceHandler> handlers) {
    Preconditions.checkArgument(!handlers.isEmpty(), "Service %s should have at least one handler", name);
    new HttpHandlerFactory("", TransactionControl.IMPLICIT).validateHttpHandler(handlers);
  }
}
