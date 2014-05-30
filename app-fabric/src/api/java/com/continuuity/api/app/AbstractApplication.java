package com.continuuity.api.app;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DatasetInstanceCreationSpec;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * A support class for {@link Application}s which reduces repetition and results in
 * a more readable configuration. Simply implement {@link #configure()} to define your application.
 */
public abstract class AbstractApplication implements Application {
  private String name;
  private String description;
  private final List<Stream> streams = Lists.newArrayList();
  private final List<DataSet> dataSets = Lists.newArrayList();
  private final Map<String, Class<? extends DatasetModule>> datasetModules = Maps.newHashMap();
  private final List<DatasetInstanceCreationSpec> datasetInstances = Lists.newArrayList();
  private final List<Flow> flows = Lists.newArrayList();
  private final Map<Procedure, Integer> procedures = Maps.newHashMap();
  private final List<MapReduce> mapReduces = Lists.newArrayList();
  private final List<Workflow> workflows = Lists.newArrayList();

  private ApplicationContext context;

  public AbstractApplication() {
    // name defaults to app class name
    this.name = this.getClass().getSimpleName();
    this.description = "";
  }

  /**
   * Override this method to configure the application.
   */
  public abstract void configure();

  @Override
  public final void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    this.context = context;

    configure();

    configurer.setName(name);
    configurer.setDescription(description);

    for (Stream stream : streams) {
      configurer.addStream(stream);
    }

    for (DataSet dataSet : dataSets) {
      configurer.addDataSet(dataSet);
    }

    for (Map.Entry<String, Class<? extends DatasetModule>> module : datasetModules.entrySet()) {
      configurer.addDatasetModule(module.getKey(), module.getValue());
    }

    for (DatasetInstanceCreationSpec instance : datasetInstances) {
      configurer.createDataSet(instance.getInstanceName(), instance.getTypeName(), instance.getProperties());
    }

    for (Flow flow : flows) {
      configurer.addFlow(flow);
    }

    for (Map.Entry<Procedure, Integer> procedure : procedures.entrySet()) {
      configurer.addProcedure(procedure.getKey(), procedure.getValue());
    }

    for (MapReduce mapReduce : mapReduces) {
      configurer.addMapReduce(mapReduce);
    }

    for (Workflow workflow : workflows) {
      configurer.addWorkflow(workflow);
    }
  }

  protected ApplicationContext getApplicationContext() {
    return context;
  }

  protected void setName(String name) {
    this.name = name;
  }

  protected void setDescription(String description) {
    this.description = description;
  }

  protected void addStream(Stream stream) {
    Preconditions.checkArgument(stream != null, "Stream cannot be null.");
    streams.add(stream);
  }

  @Deprecated
  protected void addDataSet(DataSet dataSet) {
    Preconditions.checkArgument(dataSet != null, "DataSet cannot be null.");
    dataSets.add(dataSet);
  }

  protected void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    Preconditions.checkArgument(moduleName != null, "Dataset module name cannot be null.");
    Preconditions.checkArgument(moduleClass != null, "Dataset module class cannot be null.");
    datasetModules.put(moduleName, moduleClass);
  }

  protected void createDataSet(String datasetInstanceName, String typeName, DatasetInstanceProperties properties) {
    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(typeName != null, "Dataset type name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    datasetInstances.add(new DatasetInstanceCreationSpec(datasetInstanceName, typeName, properties));
  }

  protected void createDataSet(String datasetInstanceName, String typeName) {
    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(typeName != null, "Dataset type name cannot be null.");
    datasetInstances.add(new DatasetInstanceCreationSpec(datasetInstanceName, typeName,
                                                         DatasetInstanceProperties.EMPTY));
  }

  protected void addFlow(Flow flow) {
    Preconditions.checkArgument(flow != null, "Flow cannot be null.");
    flows.add(flow);
  }

  protected void addProcedure(Procedure procedure) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    procedures.put(procedure, 1);
  }

  protected void addProcedure(Procedure procedure, int instance) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    Preconditions.checkArgument(instance >= 1, "Number of instances can't be less than 1");
    procedures.put(procedure, instance);
  }

  protected void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    mapReduces.add(mapReduce);
  }

  protected void addWorkflow(Workflow workflow) {
    Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
    workflows.add(workflow);
  }
}
