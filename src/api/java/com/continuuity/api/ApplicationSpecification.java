package com.continuuity.api;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This class provides an specification of an application to be executed within AppFabric.
 */
public final class ApplicationSpecification {

  /**
   * @return A new instance of {@link Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Name of the application.
   */
  private final String name;

  /**
   * Description of the application.
   */
  private final String description;

  /**
   * Map from stream name to {@link StreamSpecification} for all streams defined to this application.
   */
  private final Map<String, StreamSpecification> streams;

  /**
   * Map from dataset name to {@link DataSetSpecification} for all datasets defined in this application.
   */
  private final Map<String, DataSetSpecification> dataSets;

  /**
   * Map from flow name to {@link FlowSpecification} for all flows defined in this application.
   */
  private final Map<String, FlowSpecification> flows;

  /**
   * Map from procedure name to {@link ProcedureSpecification} for all procedures defined in this application.
   */
  private final Map<String, ProcedureSpecification> procedures;

  /**
   * Private constructor to only allows {@link Builder} to call it, making
   * sure this class is immutable.
   */
  private ApplicationSpecification(String name, String description,
                                   Map<String, StreamSpecification> streams,
                                   Map<String, DataSetSpecification> dataSets,
                                   Map<String, FlowSpecification> flows,
                                   Map<String, ProcedureSpecification> procedures) {
    this.name = name;
    this.description = description;
    this.streams = streams;
    this.dataSets = dataSets;
    this.flows = flows;
    this.procedures = procedures;
  }

  /**
   * @return Name of the application.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the application.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return An immutable {@link Map} from {@link Stream} name to {@link StreamSpecification}
   *         for {@link Stream}s that are configured for the application.
   */
  public Map<String, StreamSpecification> getStreams() {
    return streams;
  }

  /**
   * @return An immutable {@link Map} from {@link DataSet} name to {@link DataSetSpecification}
   *         for {@link DataSet}s that are configured for the application.
   */
  public Map<String, DataSetSpecification> getDataSets() {
    return dataSets;
  }

  /**
   * @return An immutable {@link Map} from {@link Flow} name to {@link FlowSpecification}
   *         for {@link Flow}s that are configured for the application.
   */
  public Map<String, FlowSpecification> getFlows() {
    return flows;
  }

  /**
   * @return An immutable {@link Map} from {@link Procedure} name to {@link ProcedureSpecification}
   *         for {@link Procedure}s that are configured for the application.
   */
  public Map<String, ProcedureSpecification> getProcedures() {
    return procedures;
  }

  /**
   * Builder for creating instance of {@link ApplicationSpecification}. The builder instance is
   * not reusable, meaning each instance of this class can only be used to create one instance
   * of {@link ApplicationSpecification}.
   */
  public static final class Builder {


    /**
     * @see ApplicationSpecification#name
     */
    private String name;

    /**
     * @see ApplicationSpecification#description
     */
    private String description;

    /**
     * @see ApplicationSpecification#streams
     */
    private final ImmutableMap.Builder<String, StreamSpecification> streams = ImmutableMap.builder();

    /**
     * @see ApplicationSpecification#dataSets
     */
    private final ImmutableMap.Builder<String, DataSetSpecification> dataSets = ImmutableMap.builder();

    /**
     * @see ApplicationSpecification#flows
     */
    private final ImmutableMap.Builder<String, FlowSpecification> flows = ImmutableMap.builder();

    /**
     * @see ApplicationSpecification#procedures
     */
    private final ImmutableMap.Builder<String, ProcedureSpecification> procedures = ImmutableMap.builder();

    /**
     * Sets the application name.
     *
     * @param name Name of the application.
     * @return A {@link DescriptionSetter} for setting description.
     */
    public DescriptionSetter setName(String name) {
      this.name = name;
      return new DescriptionSetter();
    }

    /**
     * Class for setting description.
     */
    public final class DescriptionSetter {

      /**
       * Sets the application description
       *
       * @param description Description of the application.
       * @return A {@link AfterDescription} for defining streams in the application.
       */
      public AfterDescription setDescription(String description) {
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Class for defining streams.
     */
    public final class AfterDescription {

      /**
       * Declares that there is {@link Stream} in the application.
       *
       * @return A {@link StreamAdder} for adding {@link Stream} to the application.
       */
      public StreamAdder withStream() {
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
     * Class for adding {@link Stream}.
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
       * Declares that there is {@link DataSet} in the application.
       *
       * @return A {@link DataSetAdder} for adding {@link DataSet} to the application.
       */
      DataSetAdder withDataSet();

      /**
       * Declares that there is no {@link DataSet} in the application.
       *
       * @return A {@link AfterDataSet} for proceeding to next configuration step.
       */
      AfterDataSet noDataSet();
    }

    /**
     * Class for adding more {@link Stream} to the application and for proceeding to next configuration step.
     */
    public final class MoreStream implements StreamAdder, AfterStream {

      @Override
      public MoreStream add(Stream stream) {
        StreamSpecification spec = stream.configure();
        streams.put(spec.getName(), spec);
        return this;
      }

      @Override
      public DataSetAdder withDataSet() {
        return new MoreDataSet();
      }

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
       * Adds a {@link DataSet} to the application.
       * @param dataset The {@link DataSet} to be included in the application.
       * @return A {@link MoreDataSet} for adding more datasets.
       */
      MoreDataSet add(DataSet dataset);
    }

    /**
     * Class for proceeding to next configuration step after {@link DataSet} configuration is completed.
     */
    public interface AfterDataSet {

      /**
       * Declares that there is {@link Flow} in the application.
       *
       * @return A {@link FlowAdder} for adding {@link Flow} to the application.
       */
      FlowAdder withFlow();

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

      @Override
      public MoreDataSet add(DataSet dataSet) {
        DataSetSpecification spec = dataSet.configure();
        dataSets.put(spec.getName(), spec);
        return this;
      }

      @Override
      public FlowAdder withFlow() {
        return new MoreFlow();
      }

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
       * Declares that there is {@link Procedure} in the application.
       *
       * @return A {@link ProcedureAdder} for adding {@link Procedure} to the application.
       */
      ProcedureAdder withProcedure();

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

      @Override
      public MoreFlow add(Flow flow) {
        FlowSpecification spec = flow.configure();
        flows.put(spec.getName(), spec);
        return this;
      }

      @Override
      public ProcedureAdder withProcedure() {
        return new MoreProcedure();
      }

      @Override
      public AfterProcedure noProcedure() {
        return new MoreProcedure();
      }
    }

    /**
     * Class for adding {@link Procedure}.
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
       * Builds the {@link ApplicationSpecification} based on what being configured.
       *
       * @return A new {@link ApplicationSpecification}.
       */
      ApplicationSpecification build();
    }

    /**
     * Class for adding more {@link Procedure} and for proceeding to next configuration step.
     */
    public final class MoreProcedure implements ProcedureAdder, AfterProcedure {

      @Override
      public MoreProcedure add(Procedure procedure) {
        ProcedureSpecification spec = procedure.configure();
        procedures.put(spec.getName(), spec);
        return this;
      }

      @Override
      public ApplicationSpecification build() {
        return new ApplicationSpecification(name, description,
                                            streams.build(), dataSets.build(),
                                            flows.build(), procedures.build());
      }
    }

    /**
     * Builder is created through {@link com.continuuity.api.ApplicationSpecification#builder()}
     */
    private Builder() { }
  }
}
