/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.app;

import co.cask.cdap.api.SingleRunnableApplication;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.service.GuavaServiceTwillRunnable;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.services.HttpServiceTwillApplication;
import co.cask.cdap.internal.batch.DefaultMapReduceSpecification;
import co.cask.cdap.internal.flow.DefaultFlowSpecification;
import co.cask.cdap.internal.procedure.DefaultProcedureSpecification;
import co.cask.cdap.internal.service.DefaultServiceSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnable;

import java.util.Arrays;
import java.util.Map;

/**
 * Default implementation of {@link ApplicationConfigurer}
 */
public class DefaultAppConfigurer implements ApplicationConfigurer {
  private String name;
  private String description;
  private final Map<String, StreamSpecification> streams = Maps.newHashMap();
  private final Map<String, String> dataSetModules = Maps.newHashMap();
  private final Map<String, DatasetCreationSpec> dataSetInstances = Maps.newHashMap();
  private final Map<String, FlowSpecification> flows = Maps.newHashMap();
  private final Map<String, ProcedureSpecification> procedures = Maps.newHashMap();
  private final Map<String, MapReduceSpecification> mapReduces = Maps.newHashMap();
  private final Map<String, WorkflowSpecification> workflows = Maps.newHashMap();
  private final Map<String, ServiceSpecification> services = Maps.newHashMap();
  // passed app to be used to resolve default name and description
  public DefaultAppConfigurer(Application app) {
    this.name = app.getClass().getSimpleName();
    this.description = "";
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
  public void addStream(Stream stream) {
    Preconditions.checkArgument(stream != null, "Stream cannot be null.");
    StreamSpecification spec = stream.configure();
    streams.put(spec.getName(), spec);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    Preconditions.checkArgument(moduleName != null, "Dataset module name cannot be null.");
    Preconditions.checkArgument(moduleClass != null, "Dataset module class cannot be null.");
    dataSetModules.put(moduleName, moduleClass.getName());
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    Preconditions.checkArgument(datasetClass != null, "Dataset class cannot be null.");
    dataSetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public void createDataset(String datasetInstanceName, String typeName, DatasetProperties properties) {
    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(typeName != null, "Dataset type name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    dataSetInstances.put(datasetInstanceName,
                         new DatasetCreationSpec(datasetInstanceName, typeName, properties));
  }

  @Override
  public void createDataset(String datasetInstanceName,
                            Class<? extends Dataset> datasetClass,
                            DatasetProperties properties) {

    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(datasetClass != null, "Dataset class name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    dataSetInstances.put(datasetInstanceName,
                         new DatasetCreationSpec(datasetInstanceName, datasetClass.getName(), properties));
    dataSetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public void addFlow(Flow flow) {
    Preconditions.checkArgument(flow != null, "Flow cannot be null.");
    FlowSpecification spec = new DefaultFlowSpecification(flow.getClass().getName(), flow.configure());
    flows.put(spec.getName(), spec);
  }

  @Override
  public void addProcedure(Procedure procedure) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, 1);
    procedures.put(spec.getName(), spec);
  }

  @Override
  public void addProcedure(Procedure procedure, int instance) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    Preconditions.checkArgument(instance >= 1, "Number of instances can't be less than 1");
    ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, instance);
    procedures.put(spec.getName(), spec);
  }

  @Override
  public void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    MapReduceSpecification spec = new DefaultMapReduceSpecification(mapReduce);
    mapReduces.put(spec.getName(), spec);
  }

  @Override
  public void addWorkflow(Workflow workflow) {
    Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
    WorkflowSpecification spec = new DefaultWorkflowSpecification(workflow.getClass().getName(),
                                                                  workflow.configure());
    workflows.put(spec.getName(), spec);

    // Add MapReduces from workflow into application
    mapReduces.putAll(spec.getMapReduce());
  }


  /**
   * Adds a Custom Service {@link TwillApplication} to the Application.
   *
   * @param application Custom Service {@link TwillApplication} to include in the Application
   */
  private void addService(TwillApplication application) {
    Preconditions.checkNotNull(application, "Service cannot be null.");

    DefaultServiceSpecification spec = new DefaultServiceSpecification(application.getClass().getName(),
                                                                       application.configure());
    services.put(spec.getName(), spec);
  }

  /**
   * Adds {@link TwillRunnable} TwillRunnable as a Custom Service {@link TwillApplication} to the Application.
   * @param runnable TwillRunnable to run as service
   * @param specification ResourceSpecification for Twill container.
   */
  private void addService(TwillRunnable runnable, ResourceSpecification specification) {
    addService(new SingleRunnableApplication(runnable, specification));
  }

  /**
   * Adds {@link com.google.common.util.concurrent.Service} as a Custom Service {@link TwillApplication}
   * to the Application.
   * @param name Name of runnable.
   * @param service Guava service to be added.
   * @param specification ResourceSpecification for Twill container.
   */
  private void addService(String name, Service service, ResourceSpecification specification) {
    addService(new GuavaServiceTwillRunnable(name, service), specification);
  }

  @Override
  public void addService(String name, Iterable<HttpServiceHandler> handlers) {
    addService(new HttpServiceTwillApplication(name, handlers));
  }

  @Override
  public void addService(String name, HttpServiceHandler handler) {
    addService(name, Arrays.asList(handler));
  }

  public ApplicationSpecification createApplicationSpec() {
    return new DefaultApplicationSpecification(name, description, streams,
                                               dataSetModules, dataSetInstances,
                                               flows, procedures, mapReduces, workflows, services);
  }
}
