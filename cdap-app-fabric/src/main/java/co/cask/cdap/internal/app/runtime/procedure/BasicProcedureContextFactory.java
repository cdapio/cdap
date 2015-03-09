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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Private interface to help creating {@link ProcedureContext}.
 */
final class BasicProcedureContextFactory {

  private final Program program;
  private final RunId runId;
  private final int instanceId;
  private final int instanceCount;
  private final Arguments userArguments;
  private final ProcedureSpecification procedureSpec;
  private final MetricsCollectionService collectionService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DatasetFramework dsFramework;

  BasicProcedureContextFactory(Program program, RunId runId, int instanceId, int instanceCount,
                               Arguments userArguments, ProcedureSpecification procedureSpec,
                               MetricsCollectionService collectionService,
                               DiscoveryServiceClient discoveryServiceClient,
                               DatasetFramework dsFramework) {
    this.program = program;
    this.runId = runId;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.userArguments = userArguments;
    this.procedureSpec = procedureSpec;
    this.collectionService = collectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.dsFramework = dsFramework;
  }

  BasicProcedureContext create() {
    return new BasicProcedureContext(program, runId, instanceId, instanceCount,
                                     procedureSpec.getDataSets(), userArguments, procedureSpec,
                                     collectionService, discoveryServiceClient, dsFramework);
  }
}
