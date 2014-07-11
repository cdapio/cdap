/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
import com.continuuity.internal.flow.DefaultFlowSpecification;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.TwillSpecification;

import java.util.HashMap;
import java.util.Map;

/**
 * Implements the ApplicationSpecifications interface configure() method,
 * and invoke the ApplicationSpecification.Builder.with() method to create a Reactor application.
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
 */
@Deprecated
public interface ApplicationSpecification {

  /**
   * @return Name of the Application.
   */
  String getName();

  /**
   * @return Description of the Application.
   */
  String getDescription();

  /**
   * @return An immutable {@link Map} from {@link Stream} name to {@link StreamSpecification}
   *         for {@link Stream}s configured for the Application.
   */
  Map<String, StreamSpecification> getStreams();

  /**
   * @return An immutable {@link Map} from {@link DataSet} name to {@link DataSetSpecification}
   *         for {@link DataSet}s configured for the Application.
   */
  Map<String, DataSetSpecification> getDataSets();

  /**
   * @return An immutable {@link Map} from {@link Flow} name to {@link FlowSpecification}
   *         for {@link Flow}s configured for the Application.
   */
  Map<String, FlowSpecification> getFlows();

  /**
   * @return An immutable {@link Map} from {@link Procedure} name to {@link ProcedureSpecification}
   *         for {@link Procedure}s configured for the Application.
   */
  Map<String, ProcedureSpecification> getProcedures();

  /**
   * @return An immutable {@link Map} from {@link MapReduce} name to {@link MapReduceSpecification}
   *         for {@link MapReduce} jobs configured for the Application.
   */
  Map<String, MapReduceSpecification> getMapReduce();

  /**
   * @return An immutable {@link Map} from {@link Workflow} name to {@link WorkflowSpecification}
   *         for {@link Workflow}s configured for the Application.
   */
  Map<String, WorkflowSpecification> getWorkflows();

  /**
   * Builder for creating instance of {@link ApplicationSpecification}. 
   * The builder instance is not reusable; each instance of this class can only 
   * be used once to create an instance of an {@link ApplicationSpecification}.
   *
   * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
   */
  public static final class Builder {

    /**
     * Name of the Application.
     */
    private String name;

    /**
     * Description of the Application.
     */
    private String description;

    /**
     * Map from Stream name to {@link StreamSpecification} for all Streams defined in this Application.
     */
    private final Map<String, StreamSpecification> streams = new HashMap<String, StreamSpecification>();

    /**
     * Map from DataSet name to {@link DataSetSpecification} for all DataSets defined in this Application.
     */
    private final Map<String, DataSetSpecification> dataSets = new HashMap<String, DataSetSpecification>();

    /**
     * Map from Flow name to {@link FlowSpecification} for all Flows defined in this Application.
     */
    private final Map<String, FlowSpecification> flows = new HashMap<String, FlowSpecification>();

    /**
     * Map from Procedure name to {@link ProcedureSpecification} for all Procedures defined in this Application.
     */
    private final Map<String, ProcedureSpecification> procedures = new HashMap<String, ProcedureSpecification>();

    /**
     * Map from {@link MapReduceSpecification} name to {@link MapReduceSpecification} 
     * for all Hadoop MapReduce jobs defined in this Application.
     */
    private final Map<String, MapReduceSpecification> mapReduces = new HashMap<String, MapReduceSpecification>();

    /**
     * Map from Workflow name to {@link WorkflowSpecification} for all Workflows defined in this Application.
     */
    private final Map<String, WorkflowSpecification> workflows = new HashMap<String, WorkflowSpecification>();

    /**
     * Map from Service name to {@link TwillSpecification} for the services defined in this Application.
     */
    private final Map<String, TwillSpecification> services = new HashMap<String, TwillSpecification>();

    /**
     * @return A new instance of {@link Builder}.
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting the Application's name.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class NameSetter {

      /**
       * Sets the Application's name.
       *
       * @param name Name of the Application.
       * @return A {@link DescriptionSetter} for setting the Application's description.
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Class for setting the Application's description.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class DescriptionSetter {

      /**
       * Sets the Application's description.
       *
       * @param description Description of the Application.
       * @return A {@link AfterDescription} for defining Streams in the Application.
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Class for defining Streams.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class AfterDescription {

      /**
       * Declares that there is a {@link Stream} in the Application.
       *
       * @return A {@link StreamAdder} for adding a {@link Stream} to the Application.
       */
      public StreamAdder withStreams() {
        return new MoreStream();
      }

      /**
       * Declares that there is no {@link Stream} in the Application.
       *
       * @return A {@link AfterStream} for proceeding to the next configuration step.
       */
      public AfterStream noStream() {
        return new MoreStream();
      }
    }

    /**
     * Class for adding a {@link Stream}.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface StreamAdder {

      /**
       * Adds a {@link Stream} to the Application.
       *
       * @param stream The {@link Stream} to be included in the Application.
       * @return A {@link MoreStream} for adding more streams.
       */
      MoreStream add(Stream stream);
    }

    /**
     * Class for proceeding to the next configuration step after {@link Stream} configuration is completed.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterStream {

      /**
       * Declares that there is a {@link DataSet} in the Application.
       *
       * @return A {@link DataSetAdder} for adding a {@link DataSet} to the Application.
       */
      DataSetAdder withDataSets();

      /**
       * Declares that there is no {@link DataSet} in the Application.
       *
       * @return A {@link AfterDataSet} for proceeding to next configuration step.
       */
      AfterDataSet noDataSet();
    }

    /**
     * Class for adding more {@link Stream}s to the Application and for proceeding to the next configuration step.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreStream implements StreamAdder, AfterStream {

      /**
       * Adds another {@link Stream} to the Application.
       * @param stream The {@link Stream} to be included in the Application.
       * @return An instance of {@link MoreStream}
       */
      @Override
      public MoreStream add(Stream stream) {
        Preconditions.checkArgument(stream != null, "Stream cannot be null.");
        StreamSpecification spec = stream.configure();
        streams.put(spec.getName(), spec);
        return this;
      }

      /**
       * Declares that the {@link Application} has one or more datasets.
       * @return An instance of {@link MoreDataSet}
       */
      @Override
      public DataSetAdder withDataSets() {
        return new MoreDataSet();
      }

      /**
       * Declares that the Application has no datasets.
       * @return An instance of {@link MoreDataSet}
       */
      @Override
      public AfterDataSet noDataSet() {
        return new MoreDataSet();
      }
    }

    /**
     * Adds a {@link DataSet} to the Application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface DataSetAdder {
      /**
       * Adds a {@link DataSet} to the Application.
       * @param dataset The {@link DataSet} to be included in the Application.
       * @return A {@link MoreDataSet} for adding more Datasets.
       */
      MoreDataSet add(DataSet dataset);
    }

    /**
     * Class for proceeding to the next configuration step after {@link DataSet} configuration is completed.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterDataSet {

      /**
       * Declares that there is a {@link Flow} in the Application.
       *
       * @return A {@link FlowAdder} for adding a {@link Flow} to the Application.
       */
      FlowAdder withFlows();

      /**
       * Declares that there is no {@link Flow} in the Application.
       *
       * @return A {@link AfterFlow} for proceeding to the next configuration step.
       */
      AfterFlow noFlow();
    }

    /**
     * Class for adding more {@link DataSet}s and for proceeding to next configuration step.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreDataSet implements DataSetAdder, AfterDataSet {

      /**
       * Adds a {@link DataSet} to the {@link Application}.
       * @param dataSet Dataset to add to {@link Application}
       * @return An instance of {@link MoreDataSet}
       */
      @Override
      public MoreDataSet add(DataSet dataSet) {
        Preconditions.checkArgument(dataSet != null, "DataSet cannot be null.");
        DataSetSpecification spec = dataSet.configure();
        dataSets.put(spec.getName(), spec);
        return this;
      }

      /**
       * Defines that {@link Application} has a {@link Flow} that is defined after a {@link DataSet}.
       * @return An instance of {@link FlowAdder}
       */
      @Override
      public FlowAdder withFlows() {
        return new MoreFlow();
      }

      /**
       * Defines that {@link Application} has no {@link Flow}.
       * @return An instance of {@link AfterFlow}
       */
      @Override
      public AfterFlow noFlow() {
        return new MoreFlow();
      }
    }

    /**
     * Class for adding {@link Flow}.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface FlowAdder {

      /**
       * Adds a {@link Flow} to the Application.
       * @param flow The {@link Flow} to be included in the Application.
       * @return A {@link MoreFlow} for adding more flows.
       */
      MoreFlow add(Flow flow);
    }

    /**
     * Class for proceeding to the next configuration step after {@link Flow} configuration is completed.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterFlow {

      /**
       * Declares that there is a {@link Procedure} in the Application.
       *
       * @return A {@link ProcedureAdder} for adding a {@link Procedure} to the Application.
       */
      ProcedureAdder withProcedures();

      /**
       * Declares that there is no {@link Procedure} in the Application.
       *
       * @return A {@link AfterProcedure} for proceeding to the next configuration step.
       */
      AfterProcedure noProcedure();
    }

    /**
     * Class for adding more {@link Flow}s and for proceeding to the next configuration step.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreFlow implements FlowAdder, AfterFlow {

      /**
       * Adds a {@link Flow} to an {@link Application}.
       * @param flow The {@link Flow} to be included in the Application.
       * @return An instance of {@link MoreFlow} for adding more {@link Flow}s to the {@link Application}
       */
      @Override
      public MoreFlow add(Flow flow) {
        Preconditions.checkArgument(flow != null, "Flow cannot be null.");
        FlowSpecification spec = new DefaultFlowSpecification(flow.getClass().getName(), flow.configure());
        flows.put(spec.getName(), spec);
        return this;
      }

      /**
       * After {@link Flow} has been added, the next step is to add a {@link Procedure}.
       * @return An instance of {@link MoreProcedure}
       */
      @Override
      public ProcedureAdder withProcedures() {
        return new MoreProcedure();
      }

      /**
       * After a {@link Flow} has been added, the next step defines that there are no more {@link Procedure}s.
       * @return An instance of {@link AfterProcedure} defining the next steps in builder.
       */
      @Override
      public AfterProcedure noProcedure() {
        return new MoreProcedure();
      }
    }

    /**
     * Class for adding a {@link Procedure}.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface ProcedureAdder {

      /**
       * Adds a {@link Procedure} to the application with one instance.
       *
       * @param procedure The {@link Procedure} to include in the application.
       * @return A {@link MoreProcedure} for adding more procedures.
       */
      MoreProcedure add(Procedure procedure);

      /**
       * Adds a {@link Procedure} to the application with the number of instances.
       *
       * @param procedure The {@link Procedure} to include in the application.
       * @param instances number of instances.
       * @return          A {@link MoreProcedure} for adding more procedures.
       */
      MoreProcedure add(Procedure procedure, int instances);
    }

    /**
     * Class for proceeding to next configuration step after {@link Procedure} configuration is completed.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterProcedure {
      /**
       * After {@link Procedure}s were added the next step is to add mapreduce jobs.
       * @return an instance of {@link MapReduceAdder}
       */
      MapReduceAdder withMapReduce();

      /**
       * After {@link Procedure}s were added the next step defines that there are no mapreduce jobs to be added.
       * @return an instance of {@link AfterMapReduce}
       */
      AfterMapReduce noMapReduce();
    }

    /**
     * Class for adding more {@link Procedure}s and for proceeding to next configuration step.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreProcedure implements ProcedureAdder, AfterProcedure {

      @Override
      public MoreProcedure add(Procedure procedure) {
        Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
        ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, 1);
        procedures.put(spec.getName(), spec);
        return this;
      }

      @Override
      public MoreProcedure add(Procedure procedure, int instance) {
        Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
        Preconditions.checkArgument(instance >= 1, "Number of instances can't be less than 1");
        ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, instance);
        procedures.put(spec.getName(), spec);
        return this;
      }

      /**
       * After {@link Procedure}s were added the next step is to add mapreduce jobs.
       * @return an instance of {@link MapReduceAdder}
       */
      @Override
      public MapReduceAdder withMapReduce() {
        return new MoreMapReduce();
      }

      /**
       * After {@link Procedure}s were added the next step specifies that there are no mapreduce jobs to be added.
       * @return an instance of {@link AfterMapReduce}
       */
      @Override
      public AfterMapReduce noMapReduce() {
        return new MoreMapReduce();
      }
    }

    /**
     * Defines interface for adding mapreduce jobs to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface MapReduceAdder {
      /**
       * Adds MapReduce job to the application. Use it when you need to re-use existing MapReduce jobs that rely on
       * Hadoop MapReduce APIs.
       * @param mapReduce The MapReduce job to add
       * @return an instance of {@link MoreMapReduce}
       */
      MoreMapReduce add(MapReduce mapReduce);
    }

    /**
     * Defines interface for proceeding to the next step after adding mapreduce jobs to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterMapReduce {

      /**
       * After {@link MapReduce} is added the next step is to workflows to application.
       *
       * @return an instance of {@link WorkflowAdder}
       */
      WorkflowAdder withWorkflows();

      /**
       * After {@link MapReduce} is added the next step specifies that there are no workflows to be added.
       *
       * @return an instance of {@link AfterWorkflow}
       */
      AfterWorkflow noWorkflow();
    }

    /**
     * Class for adding more mapreduce jobs to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreMapReduce implements MapReduceAdder, AfterMapReduce {

      @Override
      public WorkflowAdder withWorkflows() {
        return new MoreWorkflow();
      }

      @Override
      public AfterWorkflow noWorkflow() {
        return new MoreWorkflow();
      }

      /**
       * Adds a MapReduce program to the application. Use this when you need to re-use existing MapReduce programs
       * that rely on Hadoop MapReduce APIs.
       * @param mapReduce MapReduce program to add.
       * @return An instance of {@link MoreMapReduce}.
       */
      @Override
      public MoreMapReduce add(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
        MapReduceSpecification spec = new DefaultMapReduceSpecification(mapReduce);
        mapReduces.put(spec.getName(), spec);
        return this;
      }
    }

    /**
     * Define interface for adding workflow to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface WorkflowAdder {
      MoreWorkflow add(Workflow workflow);
    }

    /**
     * Defines interface for proceeding to the next step after adding workflows to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public interface AfterWorkflow {
      /**
       * Builds the {@link ApplicationSpecification} based on what is being configured.
       *
       * @return A new {@link ApplicationSpecification}.
       */
      ApplicationSpecification build();
    }

    /**
     * Class for adding Workflows to the application.
     *
     * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
     */
    public final class MoreWorkflow implements WorkflowAdder, AfterWorkflow {

      @Override
      public ApplicationSpecification build() {
        return Builder.this.build();
      }

      @Override
      public MoreWorkflow add(Workflow workflow) {
        Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
        WorkflowSpecification spec = new DefaultWorkflowSpecification(workflow.getClass().getName(),
                                                                      workflow.configure());
        workflows.put(spec.getName(), spec);

        // Add MapReduces from workflow into application
        mapReduces.putAll(spec.getMapReduce());
        return this;
      }
    }

    private ApplicationSpecification build() {
      return new ApplicationSpecificationImpl(name, description, streams, dataSets,
                                              flows, procedures, mapReduces, workflows);
    }

    /**
     * Builder is created through {@link #with()}.
     */
    private Builder() { }

    /**
     *
     */
    private final class ApplicationSpecificationImpl implements ApplicationSpecification {

      private final String name;
      private final String description;
      private final Map<String, StreamSpecification> streams;
      private final Map<String, DataSetSpecification> datasets;
      private final Map<String, FlowSpecification> flows;
      private final Map<String, ProcedureSpecification> procedures;
      private final Map<String, MapReduceSpecification> mapReduces;
      private final Map<String, WorkflowSpecification> workflows;

      public ApplicationSpecificationImpl(String name, String description,
                                          Map<String, StreamSpecification> streams,
                                          Map<String, DataSetSpecification> datasets,
                                          Map<String, FlowSpecification> flows,
                                          Map<String, ProcedureSpecification> procedures,
                                          Map<String, MapReduceSpecification> mapReduces,
                                          Map<String, WorkflowSpecification> workflows) {
        this.name = name;
        this.description = description;
        this.streams = ImmutableMap.copyOf(streams);
        this.datasets = ImmutableMap.copyOf(datasets);
        this.flows = ImmutableMap.copyOf(flows);
        this.procedures = ImmutableMap.copyOf(procedures);
        this.mapReduces = ImmutableMap.copyOf(mapReduces);
        this.workflows = ImmutableMap.copyOf(workflows);
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return description;
      }

      @Override
      public Map<String, StreamSpecification> getStreams() {
        return streams;
      }

      @Override
      public Map<String, DataSetSpecification> getDataSets() {
        return datasets;
      }

      @Override
      public Map<String, FlowSpecification> getFlows() {
        return flows;
      }

      @Override
      public Map<String, ProcedureSpecification> getProcedures() {
        return procedures;
      }

      @Override
      public Map<String, MapReduceSpecification> getMapReduce() {
        return mapReduces;
      }

      @Override
      public Map<String, WorkflowSpecification> getWorkflows() {
        return workflows;
      }
    }
  }
}
