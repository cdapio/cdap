/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactDescriptor;
import co.cask.cdap.api.artifact.Plugin;
import co.cask.cdap.api.artifact.PluginContext;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.templates.AdapterContext;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.PluginNotExistsException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.cdap.templates.AdapterPlugin;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext extends AbstractServiceDiscoverer
                                      implements DatasetContext, RuntimeContext, AdapterContext, PluginContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;
  private final List<Id> owners;
  private final Map<String, String> runtimeArguments;
  private final Map<String, Dataset> datasets;

  private final MetricsContext programMetrics;

  private final DatasetInstantiator dsInstantiator;
  private final DiscoveryServiceClient discoveryServiceClient;

  private final AdapterDefinition adapterSpec;
  private final PluginInstantiator pluginInstantiator;
  private final PluginInstantiator artifactPluginInstantiator;
  private final ArtifactRepository artifactRepository;
  private final Id.Artifact artifactId;

  /**
   * Constructs a context without application template adapter support.
   */
  protected AbstractContext(Program program, RunId runId, Arguments arguments,
                            Set<String> datasets, MetricsContext metricsContext,
                            DatasetFramework dsFramework, DiscoveryServiceClient discoveryServiceClient) {
    this(program, runId, arguments, datasets, metricsContext, dsFramework, discoveryServiceClient,
         null, null, null, null);
  }

  /**
   * Constructs a context. To have application template adapter support,
   * both the {@code adapterSpec} and {@code pluginInstantiator} must not be null.
   */
  // TODO: Pass in only one PluginInstantiator after templates, adapters are removed
  protected AbstractContext(Program program, RunId runId, Arguments arguments,
                            Set<String> datasets, MetricsContext metricsContext,
                            DatasetFramework dsFramework, DiscoveryServiceClient discoveryServiceClient,
                            @Nullable AdapterDefinition adapterSpec,
                            @Nullable PluginInstantiator pluginInstantiator,
                            @Nullable PluginInstantiator artifactPluginInstantiator,
                            ArtifactRepository artifactRepository) {
    super(program.getId());
    this.program = program;
    this.runId = runId;
    this.runtimeArguments = ImmutableMap.copyOf(arguments.asMap());
    this.discoveryServiceClient = discoveryServiceClient;
    this.owners = createOwners(program.getId(), adapterSpec);

    this.programMetrics = metricsContext;
    this.dsInstantiator = new DatasetInstantiator(program.getId().getNamespace(), dsFramework,
                                                  program.getClassLoader(), owners,
                                                  programMetrics);

    // todo: this should be instantiated on demand, at run-time dynamically. Esp. bad to do that in ctor...
    // todo: initialized datasets should be managed by DatasetContext (ie. DatasetInstantiator): refactor further
    this.datasets = Datasets.createDatasets(dsInstantiator, datasets, runtimeArguments);
    this.adapterSpec = adapterSpec;
    this.pluginInstantiator = pluginInstantiator;
    this.artifactPluginInstantiator = artifactPluginInstantiator;
    this.artifactRepository = artifactRepository;
    this.artifactId = (program.getApplicationSpecification().getArtifactId() != null) ?
      Id.Artifact.from(Id.Namespace.from(program.getNamespaceId()),
                       program.getApplicationSpecification().getArtifactId()) : null;
  }

  private List<Id> createOwners(Id.Program programId, @Nullable AdapterDefinition adapterSpec) {
    ImmutableList.Builder<Id> result = ImmutableList.builder();
    result.add(programId);
    if (adapterSpec != null) {
      result.add(Id.Adapter.from(programId.getNamespace(), adapterSpec.getName()));
    }
    return result.build();
  }

  public List<Id> getOwners() {
    return owners;
  }

  public abstract Metrics getMetrics();

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return program.getApplicationSpecification();
  }

  @Nullable
  public AdapterDefinition getAdapterSpecification() {
    return adapterSpec;
  }

  /**
   * Returns the {@link PluginInstantiator} used by this context or {@code null} if there is no plugin support.
   */
  @Nullable
  public PluginInstantiator getPluginInstantiator() {
    return pluginInstantiator;
  }

  /**
   * Returns the {@link PluginInstantiator} used by this context or {@code null} if there is no plugin support.
   */
  @Nullable
  public PluginInstantiator getArtifactPluginInstantiator() {
    return artifactPluginInstantiator;
  }

  @Override
  public String toString() {
    return String.format("namespaceId=%s, applicationId=%s, program=%s, runid=%s",
                         getNamespaceId(), getApplicationId(), getProgramName(), runId);
  }

  public MetricsContext getProgramMetrics() {
    return programMetrics;
  }

  // todo: this may be refactored further: avoid leaking dataset instantiator from context
  public DatasetInstantiator getDatasetInstantiator() {
    return dsInstantiator;
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return getDataset(name, RuntimeArguments.NO_ARGUMENTS);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    // TODO this should allow to get a dataset that was not declared with @UseDataSet. Then we can support arguments.
    try {
      @SuppressWarnings("unchecked")
      T dataset = (T) datasets.get(name);
      if (dataset != null) {
        return dataset;
      }
    } catch (Throwable t) {
      throw new DatasetInstantiationException(String.format("Can't instantiate dataset '%s'", name), t);
    }
    // if execution gets here, then dataset was null
    throw new DatasetInstantiationException(String.format("'%s' is not a known Dataset", name));
  }

  public Map<String, Dataset> getDatasets() {
    return datasets;
  }

  public String getNamespaceId() {
    return program.getNamespaceId();
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    for (Closeable ds : datasets.values()) {
      closeDataSet(ds);
    }
  }

  /**
   * Closes one dataset; logs but otherwise ignores exceptions.
   */
  protected void closeDataSet(Closeable ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.toString() + ", in context: " + this);
    }
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return getAdapterPlugin(pluginId).getProperties();
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    if (pluginInstantiator == null) {
      throw new UnsupportedOperationException("Plugin not supported for non-adapter program");
    }
    AdapterPlugin plugin = getAdapterPlugin(pluginId);
    try {
      return pluginInstantiator.loadClass(plugin.getPluginInfo(), plugin.getPluginClass());
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    if (pluginInstantiator == null) {
      throw new UnsupportedOperationException("Plugin not supported for non-adapter program");
    }
    AdapterPlugin plugin = getAdapterPlugin(pluginId);
    try {
      return pluginInstantiator.newInstance(plugin.getPluginInfo(), plugin.getPluginClass(), plugin.getProperties());
    } catch (ClassNotFoundException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns the {@link AdapterPlugin} as stored in the adapter spec for the given type and name.
   */
  private AdapterPlugin getAdapterPlugin(String pluginId) {
    if (adapterSpec == null) {
      throw new UnsupportedOperationException("Plugin not supported for non-adapter program");
    }
    AdapterPlugin plugin = adapterSpec.getPlugins().get(pluginId);
    Preconditions.checkArgument(plugin != null, "Plugin with id %s not exists in adapter %s of template %s.",
                                pluginId, adapterSpec.getName(), adapterSpec.getTemplate());
    return plugin;
  }

  public static Map<String, String> getMetricsContext(Program program, String runId) {
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(ProgramTypeMetricTag.getTagName(program.getType()), program.getName());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId);
    return tags;
  }

  public abstract Map<String, Plugin> getPlugins();

  @Override
  public PluginProperties getPluginProps(String pluginId) {
    return getPlugin(pluginId).getProperties();
  }

  @Override
  public <T> Class<T> loadClass(String pluginId) {
    if (artifactPluginInstantiator == null) {
      throw new UnsupportedOperationException("Plugin not supported for this program type");
    }
    Plugin plugin = getPlugin(pluginId);
    try {
      Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = artifactRepository.getPlugin(
        artifactId, plugin.getPluginClass().getType(), plugin.getPluginClass().getType(), plugin.getArtifactId());

      return artifactPluginInstantiator.loadClass(pluginEntry.getKey(), pluginEntry.getValue());
    } catch (ClassNotFoundException | PluginNotExistsException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> T newInstance(String pluginId) throws InstantiationException {
    if (artifactPluginInstantiator == null) {
      throw new UnsupportedOperationException("Plugin not supported for this program type");
    }
    Plugin plugin = getPlugin(pluginId);
    try {
      Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = artifactRepository.getPlugin(
        artifactId, plugin.getPluginClass().getType(), plugin.getPluginClass().getName(), plugin.getArtifactId());

      return artifactPluginInstantiator.newInstance(pluginEntry.getKey(), pluginEntry.getValue(),
                                                    plugin.getProperties());
    } catch (ClassNotFoundException | PluginNotExistsException e) {
      // Shouldn't happen, unless there is bug in file localization
      throw new IllegalArgumentException("Plugin class not found", e);
    } catch (IOException e) {
      // This is fatal, since jar cannot be expanded.
      throw Throwables.propagate(e);
    }
  }

  private Plugin getPlugin(String pluginId) {
    if (getPlugins() == null) {
      throw new UnsupportedOperationException("Plugin not supported in this program");
    }

    Plugin plugin = getPlugins().get(pluginId);
    Preconditions.checkArgument(plugin != null, "Plugin with id %s does not exist in program %s of application %s.",
                                pluginId, program.getId(), program.getApplicationId());
    return plugin;
  }
}
