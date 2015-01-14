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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.ProcedureLoggingContext;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;
import java.util.Set;

/**
 * Procedure runtime context
 */
final class BasicProcedureContext extends AbstractContext implements ProcedureContext {

  private final String procedureId;
  private final int instanceId;
  private volatile int instanceCount;

  private final ProcedureSpecification procedureSpec;
  private final Metrics userMetrics;
  private final ProcedureLoggingContext procedureLoggingContext;

  BasicProcedureContext(Program program, RunId runId, int instanceId, int instanceCount,
                        Set<String> datasets, Arguments runtimeArguments,
                        ProcedureSpecification procedureSpec, MetricsCollectionService collectionService,
                        DiscoveryServiceClient discoveryServiceClient,
                        DatasetFramework dsFramework, CConfiguration conf) {
    super(program, runId, runtimeArguments, datasets,
          getMetricCollector(collectionService, MetricsScope.SYSTEM, program, runId.getId(), instanceId),
          dsFramework, conf, discoveryServiceClient);
    this.procedureId = program.getName();
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.procedureSpec = procedureSpec;
    this.userMetrics = new ProgramUserMetrics(getMetricCollector(collectionService, MetricsScope.USER,
                                                                 program, runId.getId(), instanceId));
    this.procedureLoggingContext = new ProcedureLoggingContext(getNamespaceId(), getApplicationId(), getProcedureId());
  }

  @Override
  public String toString() {
    return String.format("procedure=%s, instance=%d, %s", getProcedureId(), getInstanceId(), super.toString());
  }

  @Override
  public ProcedureSpecification getSpecification() {
    return procedureSpec;
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  public String getProcedureId() {
    return procedureId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return procedureLoggingContext;
  }

  private static MetricsCollector getMetricCollector(MetricsCollectionService service,
                                                     MetricsScope scope, Program program,
                                                     String runId, int instanceId) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId));
    return service.getCollector(scope, tags);
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }
}
