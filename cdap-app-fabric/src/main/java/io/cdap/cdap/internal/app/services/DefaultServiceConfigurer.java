/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.SystemTableConfigurer;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.service.ServiceConfigurer;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.SystemServiceConfigurer;
import io.cdap.cdap.api.service.http.HttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.SystemHttpServiceConfigurer;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;

import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link ServiceConfigurer}.
 */
public class DefaultServiceConfigurer extends AbstractConfigurer implements SystemServiceConfigurer {

  private final String className;
  private final Id.Artifact artifactId;
  private final PluginFinder pluginFinder;
  private final PluginInstantiator pluginInstantiator;
  private final SystemTableConfigurer systemTableConfigurer;

  private String name;
  private String description;
  private List<HttpServiceHandler> handlers;
  private Resources resources;
  private int instances;

  /**
   * Create an instance of {@link DefaultServiceConfigurer}
   */
  public DefaultServiceConfigurer(Service service, Id.Namespace namespace, Id.Artifact artifactId,
                                  PluginFinder pluginFinder, PluginInstantiator pluginInstantiator,
                                  DefaultSystemTableConfigurer systemTableConfigurer) {
    super(namespace, artifactId, pluginFinder, pluginInstantiator);
    this.className = service.getClass().getName();
    this.name = service.getClass().getSimpleName();
    this.description = "";
    this.handlers = Lists.newArrayList();
    this.resources = new Resources();
    this.instances = 1;
    this.artifactId = artifactId;
    this.pluginFinder = pluginFinder;
    this.pluginInstantiator = pluginInstantiator;
    this.systemTableConfigurer = systemTableConfigurer;
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
    return new ServiceSpecification(className, name, description, handleSpecs, resources, instances, getPlugins());
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
        handler, deployNamespace, artifactId, pluginFinder, pluginInstantiator, systemTableConfigurer);
      handler.configure(configurer);
      HttpServiceHandlerSpecification spec = configurer.createSpecification();
      Preconditions.checkArgument(!handleSpecs.containsKey(spec.getName()),
                                  "Handler with name %s already existed.", spec.getName());
      handleSpecs.put(spec.getName(), spec);
      addDatasetModules(configurer.getDatasetModules());
      addDatasetSpecs(configurer.getDatasetSpecs());
      addPlugins(configurer.getPlugins());
    }
    return handleSpecs;
  }

  private void verifyHandlers(List<? extends HttpServiceHandler> handlers) {
    Preconditions.checkArgument(!handlers.isEmpty(), "Service %s should have at least one handler", name);
    new HttpHandlerFactory("", TransactionControl.IMPLICIT).validateHttpHandler(handlers);
    for (HttpServiceHandler handler : handlers) {
      // check that a system service handler is only used in system namespace
      if (!deployNamespace.equals(Id.Namespace.fromEntityId(NamespaceId.SYSTEM))) {
        TypeToken<?> contextType = TypeToken.of(handler.getClass())
          .resolveType(HttpServiceHandler.class.getTypeParameters()[0]);
        if (SystemHttpServiceContext.class.isAssignableFrom(contextType.getRawType())) {
          throw new IllegalArgumentException(String.format(
            "Invalid service handler '%s'. Service handlers can only use a SystemHttpServiceContext if the "
              + "application is deployed in the system namespace.", handler.getClass().getSimpleName()));
        }
        TypeToken<?> configurerType = TypeToken.of(handler.getClass())
          .resolveType(HttpServiceHandler.class.getTypeParameters()[1]);
        if (SystemHttpServiceConfigurer.class.isAssignableFrom(configurerType.getRawType())) {
          throw new IllegalArgumentException(String.format(
            "Invalid service handler '%s'. Service handlers can only use a SystemHttpServiceConfigurer if the "
              + "application is deployed in the system namespace.", handler.getClass().getSimpleName()));
        }
      }
    }
  }

  @Override
  public void createTable(StructuredTableSpecification tableSpecification) {
    systemTableConfigurer.createTable(tableSpecification);
  }
}
