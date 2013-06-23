/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.DefaultApplicationSpecification;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
import com.continuuity.internal.flow.DefaultFlowSpecification;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides a specification of an application to be executed within AppFabric.
 */
public interface ApplicationSpecification {

  /**
   * @return Name of the application.
   */
  String getName();

  /**
   * @return Description of the application.
   */
  String getDescription();

  /**
   * @return An immutable {@link Map} from {@link Stream} name to {@link StreamSpecification}
   *         for {@link Stream}s that are configured for the application.
   */
  Map<String, StreamSpecification> getStreams();

  /**
   * @return An immutable {@link Map} from {@link DataSet} name to {@link DataSetSpecification}
   *         for {@link DataSet}s that are configured for the application.
   */
  Map<String, DataSetSpecification> getDataSets();

  /**
   * @return An immutable {@link Map} from {@link Flow} name to {@link FlowSpecification}
   *         for {@link Flow}s that are configured for the application.
   */
  Map<String, FlowSpecification> getFlows();

  /**
   * @return An immutable {@link Map} from {@link Procedure} name to {@link ProcedureSpecification}
   *         for {@link Procedure}s that are configured for the application.
   */
  Map<String, ProcedureSpecification> getProcedures();

  /**
   * @return An immutable {@link Map} from {@link com.continuuity.api.batch.MapReduceSpecification} name to
   *         {@link com.continuuity.api.batch.MapReduceSpecification}
   */
  Map<String, MapReduceSpecification> getMapReduces();

  /**
   * Builder for creating instance of {@link ApplicationSpecification}. The builder instance is
   * not reusable, meaning each instance of this class can only be used to create one instance
   * of {@link ApplicationSpecification}.
   */
  public static final class Builder {

    /**
     * Name of the application.
     */
    private String name;

    /**
     * Description of the application.
     */
    private String description;

    /**
     * Map from stream name to {@link StreamSpecification} for all streams defined to this application.
     */
    private final Map<String, StreamSpecification> streams = new HashMap<String, StreamSpecification>();

    /**
     * Map from dataset name to {@link DataSetSpecification} for all datasets defined in this application.
     */
    private final Map<String, DataSetSpecification> dataSets = new HashMap<String, DataSetSpecification>();

    /**
     * Map from flow name to {@link FlowSpecification} for all flows defined in this application.
     */
    private final Map<String, FlowSpecification> flows = new HashMap<String, FlowSpecification>();

    /**
     * Map from procedure name to {@link ProcedureSpecification} for all procedures defined in this application.
     */
    private final Map<String, ProcedureSpecification> procedures = new HashMap<String, ProcedureSpecification>();

    /**
     * Map from {@link com.continuuity.api.batch.MapReduceSpecification} name to {@link com.continuuity.api.batch.MapReduceSpecification} for all
     * Hadoop mapreduce jobs defined in this application
     */
    private final Map<String, MapReduceSpecification> mapReduces =
      new HashMap<String, MapReduceSpecification>();

    /**
     * @return A new instance of {@link Builder}.
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting name.
     */
    public final class NameSetter {

      /**
       * Sets the application name.
       *
       * @param name Name of the application.
       * @return A {@link DescriptionSetter} for setting description.
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Class for setting description.
     */
    public final class DescriptionSetter {

      /**
       * Sets the Application description.
       *
       * @param description Description of the Application.
       * @return A {@link AfterDescription} for defining streams in the application.
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Class for defining streams.
     */
    public final class AfterDescription {

      /**
       * Declares that there is a {@link Stream} in the application.
       *
       * @return A {@link StreamAdder} for adding a {@link Stream} to the application.
       */
      public StreamAdder withStreams() {
        return new MoreStream();
      }

      /**
       * Declares that there is no {@link Stream} in the application.
       *
       * @return A {@link AfterStream} for proceeding to next configuration step.
       */
      public AfterStream noStream() {
        return new MoreStream();
      }
    }

    /**
     * Class for adding a {@link Stream}.
     */
    public interface StreamAdder {

      /**
       * Adds a {@link Stream} to the application.
       *
       * @param stream The {@link Stream} to be included in the application.
       * @return A {@link MoreStream} for adding more streams.
       */
      MoreStream add(Stream stream);
    }

    /**
     * Class for proceeding to next configuration step after {@link Stream} configuration is completed.
     */
    public interface AfterStream {

      /**
       * Declares that there is a {@link DataSet} in the application.
       *
       * @return A {@link DataSetAdder} for adding a {@link DataSet} to the application.
       */
      DataSetAdder withDataSets();

      /**
       * Declares that there is no {@link DataSet} in the application.
       *
       * @return A {@link AfterDataSet} for proceeding to next configuration step.
       */
      AfterDataSet noDataSet();
    }

    /**
     * Class for adding more {@link Stream}s to the application and for proceeding to next configuration step.
     */
    public final class MoreStream implements StreamAdder, AfterStream {

      /**
       * Adds another {@link Stream} to the application.
       * @param stream The {@link Stream} to be included in the application.
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
       * Sets if the {@link Application} has Datasets or not.
       * @return An instance of {@link MoreDataSet}
       */
      @Override
      public DataSetAdder withDataSets() {
        return new MoreDataSet();
      }

      /**
       * Defines what needs to happen after adding a {@link DataSet}
       * @return An instance of {@link MoreDataSet}
       */
      @Override
      public AfterDataSet noDataSet() {
        return new MoreDataSet();
      }
    }

    /**
     * Class for adding {@link DataSet}.
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
     * Class for proceeding to next configuration step after {@link DataSet} configuration is completed.
     */
    public interface AfterDataSet {

      /**
       * Declares that there is a {@link Flow} in the application.
       *
       * @return A {@link FlowAdder} for adding a {@link Flow} to the application.
       */
      FlowAdder withFlows();

      /**
       * Declares that there is no {@link Flow} in the application.
       *
       * @return A {@link AfterFlow} for proceeding to next configuration step.
       */
      AfterFlow noFlow();
    }

    /**
     * Class for adding more {@link DataSet} and for proceeding to next configuration step.
     */
    public final class MoreDataSet implements DataSetAdder, AfterDataSet {

      /**
       * Adds a {@link DataSet} to the {@link Application}
       * @param dataSet to be added to {@link Application}
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
       * Defines that {@link Application} has a {@link Flow} that is defined after a {@link DataSet}
       * @return An instance of {@link FlowAdder}
       */
      @Override
      public FlowAdder withFlows() {
        return new MoreFlow();
      }

      /**
       * Defines that {@link Application} has no {@link Flow}
       * @return An instance of {@link AfterFlow}
       */
      @Override
      public AfterFlow noFlow() {
        return new MoreFlow();
      }
    }

    /**
     * Class for adding {@link Flow}.
     */
    public interface FlowAdder {

      /**
       * Adds a {@link Flow} to the application.
       * @param flow The {@link Flow} to be included in the application.
       * @return A {@link MoreFlow} for adding more flows.
       */
      MoreFlow add(Flow flow);
    }

    /**
     * Class for proceeding to next configuration step after {@link Flow} configuration is completed.
     */
    public interface AfterFlow {

      /**
       * Declares that there is a {@link Procedure} in the application.
       *
       * @return A {@link ProcedureAdder} for adding a {@link Procedure} to the application.
       */
      ProcedureAdder withProcedures();

      /**
       * Declares that there is no {@link Procedure} in the application.
       *
       * @return A {@link AfterProcedure} for proceeding to next configuration step.
       */
      AfterProcedure noProcedure();
    }

    /**
     * Class for adding more {@link Flow} and for proceeding to next configuration step.
     */
    public final class MoreFlow implements FlowAdder, AfterFlow {

      /**
       * Adds a {@link Flow} to an {@link Application}
       * @param flow The {@link Flow} to be included in the application.
       * @return An instance of {@link MoreFlow} allowing to add more {@link Flow}s to the {@link Application}
       */
      @Override
      public MoreFlow add(Flow flow) {
        Preconditions.checkArgument(flow != null, "Flow cannot be null.");
        FlowSpecification spec = new DefaultFlowSpecification(flow.getClass().getName(), flow.configure());
        flows.put(spec.getName(), spec);
        return this;
      }

      /**
       * After {@link Flow} has been added, next step is to add a {@link Procedure}
       * @return An instance of {@link MoreProcedure}
       */
      @Override
      public ProcedureAdder withProcedures() {
        return new MoreProcedure();
      }

      /**
       * After {@link Flow} has been added, next step defines that there are no more {@link Procedure}s
       * @return An instance of {@link AfterProcedure} defining next steps in builder.
       */
      @Override
      public AfterProcedure noProcedure() {
        return new MoreProcedure();
      }
    }

    /**
     * Class for adding a {@link Procedure}.
     */
    public interface ProcedureAdder {

      /**
       * Adds a {@link Procedure} to the application.
       *
       * @param procedure The {@link Procedure} to be included in the application.
       * @return A {@link MoreProcedure} for adding more procedures.
       */
      MoreProcedure add(Procedure procedure);
    }

    /**
     * Class for proceeding to next configuration step after {@link Procedure} configuration is completed.
     */
    public interface AfterProcedure {
      /**
       * Builds the {@link ApplicationSpecification} based on what is being configured.
       *
       * @return A new {@link ApplicationSpecification}.
       */
      @Deprecated
      ApplicationSpecification build();

      /**
       * After {@link Procedure}s were added the next step is to add batch jobs
       * @return an instance of {@link BatchAdder}
       */
      BatchAdder withBatch();

      /**
       * After {@link Procedure}s were added the next step defines that there are no batch jobs to be added
       * @return an instance of {@link AfterBatch}
       */
      AfterBatch noBatch();
    }

    /**
     * Class for adding more {@link Procedure} and for proceeding to next configuration step.
     */
    public final class MoreProcedure implements ProcedureAdder, AfterProcedure {

      /**
       * Adds a {@link Procedure} to the {@link Application}
       * @param procedure The {@link Procedure} to be included in the application.
       * @return An instance of {@link MoreProcedure}
       */
      @Override
      public MoreProcedure add(Procedure procedure) {
        Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
        ProcedureSpecification spec = new DefaultProcedureSpecification(procedure);
        procedures.put(spec.getName(), spec);
        return this;
      }

      /**
       * Defines a builder for {@link FlowSpecification}
       * @return An instance of {@link FlowSpecification}
       */
      @Deprecated
      @Override
      public ApplicationSpecification build() {
        return new DefaultApplicationSpecification(name, description, streams, dataSets,
                                                   flows, procedures, mapReduces);
      }

      /**
       * After {@link Procedure}s were added the next step is to add batch jobs
       * @return an instance of {@link BatchAdder}
       */
      @Override
      public BatchAdder withBatch() {
        return new MoreBatch();
      }

      /**
       * After {@link Procedure}s were added the next step defines that there are no batch jobs to be added
       * @return an instance of {@link AfterBatch}
       */
      @Override
      public AfterBatch noBatch() {
        return new MoreBatch();
      }
    }

    /**
     * Defines interface for adding batch jobs to the application
     */
    public interface BatchAdder {
      /**
       * Adds mapreduce job to the application. Use it when you need to re-use existing mapreduce jobs which rely on
       * Hadoop mapreduce APIs.
       * @param mapReduce job to add
       * @return an instance of {@link MoreBatch}
       */
      MoreBatch add(MapReduce mapReduce);
    }

    /**
     * Defines interface for proceeding to the next step after adding batch jobs to the application
     */
    public interface AfterBatch {
      /**
       * Builds the {@link ApplicationSpecification} based on what is being configured.
       *
       * @return A new {@link ApplicationSpecification}.
       */
      ApplicationSpecification build();
    }

    /**
     * Class for adding more batch jobs to the application
     */
    public final class MoreBatch implements BatchAdder, AfterBatch {
      /**
       * Builds the {@link ApplicationSpecification} based on what is being configured.
       *
       * @return A new {@link ApplicationSpecification}.
       */
      @Override
      public ApplicationSpecification build() {
        return new DefaultApplicationSpecification(name, description, streams, dataSets,
                                                   flows, procedures, mapReduces);
      }

      /**
       * Adds mapreduce job to the application. Use it when you need to re-use existing mapreduce jobs which rely on
       * Hadoop mapreduce APIs.
       * @param mapReduce job to add
       * @return an instance of {@link MoreBatch}
       */
      @Override
      public MoreBatch add(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
        MapReduceSpecification spec = new DefaultMapReduceSpecification(mapReduce);
        mapReduces.put(spec.getName(), spec);
        return this;
      }
    }

    /**
     * Builder is created through {@link #with()}
     */
    private Builder() { }
  }
}
