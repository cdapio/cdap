/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.cdap.app.metrics.ServiceRunnableMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of HttpServiceContext which simply stores and retrieves the
 * spec provided when this class is instantiated
 */
public class BasicHttpServiceContext extends AbstractContext implements TransactionalHttpServiceContext {

  private final HttpServiceSpecification spec;
  private final Map<String, String> runtimeArgs;
  private final TransactionContext txContext;
  private final ServiceRunnableMetrics serviceRunnableMetrics;

  /**
   * Instantiates the context with a spec and a array of runtime arguments.
   *
   * @param spec the {@link HttpServiceSpecification} for this context.
   * @param runtimeArgs the runtime arguments as a list of strings.
   */
  public BasicHttpServiceContext(HttpServiceSpecification spec, String[] runtimeArgs, Program program, RunId runId,
                                 Set<String> datasets, String metricsContext,
                                 MetricsCollectionService metricsCollectionService, DatasetFramework dsFramework,
                                 CConfiguration conf,
                                 DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient) {
    super(program, runId, datasets, metricsContext, metricsCollectionService, dsFramework, conf,
          discoveryServiceClient);
    this.spec = spec;
    this.runtimeArgs = ImmutableMap.copyOf(RuntimeArguments.fromPosixArray(runtimeArgs));
    this.txContext = new TransactionContext(txClient, getDatasetInstantiator().getTransactionAware());
    this.serviceRunnableMetrics = new ServiceRunnableMetrics(metricsCollectionService, metricsContext);
  }

  /**
   * @return the {@link HttpServiceSpecification} for this context
   */
  @Override
  public HttpServiceSpecification getSpecification() {
    return spec;
  }

  /**
   * @return the runtime arguments for the {@link HttpServiceContext}
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }

  @Override
  public Metrics getMetrics() {
    return serviceRunnableMetrics;
  }

  @Override
  public TransactionContext getTransactionContext() {
    return txContext;
  }
}
