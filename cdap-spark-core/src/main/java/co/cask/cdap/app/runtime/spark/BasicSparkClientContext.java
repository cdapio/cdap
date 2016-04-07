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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.spark.SparkConf;
import org.apache.twill.api.RunId;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkClientContext} only being used during {@link Spark#beforeSubmit(SparkClientContext)}
 * and {@link Spark#onFinish(boolean, SparkClientContext)} calls.
 */
final class BasicSparkClientContext implements SparkClientContext {

  private final SparkRuntimeContext sparkRuntimeContext;
  private final Map<String, LocalizeResource> localizeResources;
  private Resources executorResources;
  private SparkConf sparkConf;

  BasicSparkClientContext(SparkRuntimeContext sparkRuntimeContext) {
    this.sparkRuntimeContext = sparkRuntimeContext;
    this.localizeResources = new HashMap<>();
    this.executorResources = Optional.fromNullable(
      sparkRuntimeContext.getSparkSpecification().getExecutorResources()).or(new Resources());
  }

  @Override
  public SparkSpecification getSpecification() {
    return sparkRuntimeContext.getSparkSpecification();
  }

  @Override
  public long getLogicalStartTime() {
    return sparkRuntimeContext.getLogicalStartTime();
  }

  @Override
  public ServiceDiscoverer getServiceDiscoverer() {
    return sparkRuntimeContext;
  }

  @Override
  public Metrics getMetrics() {
    return sparkRuntimeContext;
  }

  @Override
  public PluginContext getPluginContext() {
    return sparkRuntimeContext;
  }

  @Override
  public void setExecutorResources(Resources resources) {
    this.executorResources = resources;
  }

  @Override
  public <T> void setSparkConf(T sparkConf) {
    this.sparkConf = (SparkConf) sparkConf;
  }

  @Nullable
  @Override
  public WorkflowToken getWorkflowToken() {
    WorkflowProgramInfo workflowProgramInfo = sparkRuntimeContext.getWorkflowInfo();
    return workflowProgramInfo == null ? null : workflowProgramInfo.getWorkflowToken();
  }

  @Override
  public void localize(String name, URI uri) {
    localize(name, uri, false);
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    try {
      URI actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), name);
      localizeResources.put(name, new LocalizeResource(actualURI, archive));
    } catch (URISyntaxException e) {
      // Most of the URI is constructed from the passed URI. So ideally, this should not happen.
      // If it does though, there is nothing that clients can do to recover, so not propagating a checked exception.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return sparkRuntimeContext.getApplicationSpecification();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return sparkRuntimeContext.getRuntimeArguments();
  }

  @Override
  public String getNamespace() {
    return sparkRuntimeContext.getNamespace();
  }

  @Override
  public RunId getRunId() {
    return sparkRuntimeContext.getRunId();
  }

  @Override
  public Admin getAdmin() {
    return sparkRuntimeContext.getAdmin();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    sparkRuntimeContext.getDatasetCache().releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    sparkRuntimeContext.getDatasetCache().discardDataset(dataset);
  }

  /**
   * Returns all the {@link LocalizeResource} that need to be localized for the Spark execution.
   *
   * @return a {@link Map} from resource name to {@link LocalizeResource}.
   */
  Map<String, LocalizeResource> getLocalizeResources() {
    return localizeResources;
  }

  /**
   * Returns the {@link Resources} requirement to be used for the Spark executor processes.
   */
  Resources getExecutorResources() {
    return executorResources;
  }

  /**
   * Returns the extra {@link SparkConf} to be passed to Spark submit.
   */
  @Nullable
  SparkConf getSparkConf() {
    return sparkConf;
  }
}
