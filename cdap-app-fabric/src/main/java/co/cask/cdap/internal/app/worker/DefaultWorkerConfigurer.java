/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerConfigurer;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.internal.app.AbstractConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the {@link WorkerConfigurer}.
 */
public class DefaultWorkerConfigurer extends AbstractConfigurer implements WorkerConfigurer {

  private final Worker worker;
  private String name;
  private String description;
  private Resources resource;
  private int instances;
  private Map<String, String> properties;
  private Set<String> datasets;

  public DefaultWorkerConfigurer(Worker worker, Id.Namespace deployNamespace, Id.Artifact artifactId,
                                 ArtifactRepository artifactRepository,
                                 PluginInstantiator pluginInstantiator) {
    super(deployNamespace, artifactId, artifactRepository, pluginInstantiator);
    this.worker = worker;
    this.name = worker.getClass().getSimpleName();
    this.description = "";
    this.resource = new Resources();
    this.instances = 1;
    this.properties = new HashMap<>();
    this.datasets = Sets.newHashSet();
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
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources cannot be null.");
    this.resource = resources;
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instances must be > 0.");
    this.instances = instances;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }

  public WorkerSpecification createSpecification() {
    // Grab all @Property fields
    Reflections.visit(worker, worker.getClass(), new PropertyFieldExtractor(properties));
    return new WorkerSpecification(worker.getClass().getName(), name, description,
                                   properties, datasets, resource, instances);
  }
}
