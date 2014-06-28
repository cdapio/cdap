/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.flow.DefaultFlowSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * This class provides the specification of a Flow. Instances of this class should be created through
 * the {@link Builder} class by invoking the {@link Builder#with()} method.
 *
 * <p>
 * Example FlowSpecification:
 *
 *  <pre>
 *    <code>
 *      public class PurchaseFlow implements Flow {
 *        {@literal @}Override
 *        public FlowSpecification configure() {
 *          return FlowSpecification.Builder.with()
 *            .setName("PurchaseFlow")
 *            .setDescription("Reads user and purchase information and stores in dataset")
 *            .withFlowlets()
 *              .add("reader", new PurchaseStreamReader())
 *              .add("collector", new PurchaseStore())
 *            .connect()
 *              .fromStream("purchaseStream").to("reader")
 *              .from("reader").to("collector")
 *            .build();
 *        }
 *      }
 *    </code> 
 *  </pre>
 *
 * See the <i>Continuuity Reactor Developer Guide</i> and the Reactor example applications.
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 *
 */
public interface FlowSpecification extends ProgramSpecification {

  /**
   * @return Immutable Map from flowlet name to {@link FlowletDefinition}.
   */
  Map<String, FlowletDefinition> getFlowlets();

  /**
   * @return Immutable list of {@link FlowletConnection}s.
   */
  List<FlowletConnection> getConnections();

  /**
   * Defines a builder for building connections or topology for a flow.
   */
  static final class Builder {
    private String name;
    private String description;
    private final Map<String, FlowletDefinition> flowlets = Maps.newHashMap();
    private final List<FlowletConnection> connections = Lists.newArrayList();

    /**
     * Creates a {@link Builder} for building instance of {@link FlowSpecification}.
     *
     * @return a new builder instance.
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting flow name.
     */
    public final class NameSetter {

      /**
       * Sets the name of the Flow.
       * @param name of the flow.
       * @return An instance of {@link DescriptionSetter}
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, UserMessages.getMessage(UserErrors.FLOW_SPEC_NAME));

        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Defines a class for defining the flow description.
     */
    public final class DescriptionSetter {

      /**
       * Sets the description for the flow.
       * @param description of the flow.
       * @return A instance of {@link AfterDescription}
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, UserMessages.getMessage(UserErrors.FLOW_SPEC_DESC));
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Defines a class that represents what needs to happen after a description
     * has been added.
     */
    public final class AfterDescription {

      /**
       * @return An instance of {@link FlowletAdder} for adding flowlets to specification.
       */
      public FlowletAdder withFlowlets() {
        return new MoreFlowlet();
      }
    }

    /**
     * FlowletAdder is responsible for capturing the information of a Flowlet during the
     * specification creation.
     */
    public interface FlowletAdder {

      /**
       * Add a flowlet to the flow.
       * @param flowlet {@link Flowlet} instance to be added to flow.
       * @return An instance of {@link MoreFlowlet} for adding more flowlets
       */
      MoreFlowlet add(Flowlet flowlet);

      /**
       * Add a flowlet to the flow with the minimum number of instances of the flowlet to start with.
       * @param flowlet {@link Flowlet} instance to be added to flow.
       * @param instances Number of instances for the flowlet
       * @return An instance of {@link MoreFlowlet} for adding more flowlets
       */
      MoreFlowlet add(Flowlet flowlet, int instances);

      /**
       * Add a flowlet to flow with the specified name. The specified name overrides the one
       * in {@link com.continuuity.api.flow.flowlet.FlowletSpecification#getName() FlowletSpecification.getName()}
       * returned by {@link Flowlet#configure()}.
       * @param name Name of the flowlet
       * @param flowlet {@link Flowlet} instance to be added to flow.
       * @return An instance of {@link MoreFlowlet} for adding more flowlets.
       */
      MoreFlowlet add(String name, Flowlet flowlet);

      /**
       * Add a flowlet to flow with the specified name with minimum number of instances to start with.
       * The name specified overrides the one
       * in {@link com.continuuity.api.flow.flowlet.FlowletSpecification#getName() FlowletSpecification.getName()}
       * returned by {@link Flowlet#configure()}.
       * @param name Name of the flowlet
       * @param flowlet {@link Flowlet} instance to be added to flow.
       * @param instances Number of instances for the flowlet
       * @return An instance of {@link MoreFlowlet} for adding more flowlets.
       */
      MoreFlowlet add(String name, Flowlet flowlet, int instances);
    }

    /**
     * This class allows more flowlets to be defined. This is part of a controlled builder.
     */
    public final class MoreFlowlet implements FlowletAdder {

      @Override
      public MoreFlowlet add(Flowlet flowlet) {
        return add(flowlet, 1);
      }

      @Override
      public MoreFlowlet add(final Flowlet flowlet, int instances) {
        return add(null, flowlet, instances);
      }

      @Override
      public MoreFlowlet add(String name, Flowlet flowlet) {
        return add(name, flowlet, 1);
      }

      @Override
      public MoreFlowlet add(String name, Flowlet flowlet, int instances) {

        Preconditions.checkArgument(flowlet != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));

        FlowletDefinition flowletDef = new FlowletDefinition(name, flowlet, instances);
        String flowletName = flowletDef.getFlowletSpec().getName();

        Preconditions.checkArgument(instances > 0, String.format(UserMessages.getMessage(UserErrors.INVALID_INSTANCES),
          flowletName, instances));

        Preconditions.checkArgument(!flowlets.containsKey(flowletName),
                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_EXISTS), flowletName);

        flowlets.put(flowletName, flowletDef);

        return this;
      }

      /**
       * Defines a connection between two flowlets.
       * @return An instance of {@link ConnectFrom}
       */
      public ConnectFrom connect() {
        return new Connector();
      }
    }

    /**
     * Defines the starting flowlet for a connection.
     */
    public interface ConnectFrom {

      /**
       * Defines the flowlet that is at the beginning of the connection.
       * @param flowlet that is at the beginning of connection.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Flowlet flowlet);

      /**
       * Defines the stream that the connection is reading from.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Stream stream);

      /**
       * Defines the flowlet that is at the beginning of the connection by the flowlet name.
       * @param flowlet Name of the flowlet.
       * @return And instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(String flowlet);

      /**
       * Defines the stream that the connection is reading from by the stream name.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo fromStream(String stream);
    }

    /**
     * Class defining the ConnectTo interface for a connection.
     */
    public interface ConnectTo {

      /**
       * Defines the flowlet that the connection is connecting to.
       * @param flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      MoreConnect to(Flowlet flowlet);

      /**
       * Defines the flowlet that connection is connecting to by the flowlet name.
       * @param flowlet Name of the flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      MoreConnect to(String flowlet);
    }

    /**
     * Interface that defines the building of FlowSpecification.
     */
    public interface MoreConnect extends ConnectFrom {
      /**
       * Constructs a {@link FlowSpecification}.
       * @return An instance of {@link FlowSpecification}
       */
      FlowSpecification build();
    }

    /**
     * Class that defines the connection between two flowlets.
     */
    public final class Connector implements ConnectFrom, ConnectTo, MoreConnect {

      private String fromStream;
      private FlowletDefinition fromFlowlet;

      @Override
      public ConnectTo from(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
        return from(flowlet.configure().getName());
      }

      @Override
      public ConnectTo from(Stream stream) {
        Preconditions.checkArgument(stream != null, UserMessages.getMessage(UserErrors.INVALID_STREAM_NULL));
        return fromStream(stream.configure().getName());
      }

      @Override
      public ConnectTo from(String flowlet) {
        Preconditions.checkArgument(flowlets.containsKey(flowlet),
                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NAME), flowlet);
        fromFlowlet = flowlets.get(flowlet);
        fromStream = null;
        return this;
      }

      @Override
      public ConnectTo fromStream(String stream) {
        Preconditions.checkArgument(stream != null, UserMessages.getMessage(UserErrors.INVALID_STREAM_NAME), stream);
        fromFlowlet = null;
        fromStream = stream;
        return this;
      }

      @Override
      public MoreConnect to(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
        return to(flowlet.configure().getName());
      }

      @Override
      public MoreConnect to(String flowlet) {
        Preconditions.checkArgument(flowlet != null, UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NULL));
        Preconditions.checkArgument(flowlets.containsKey(flowlet),
                UserMessages.getMessage(UserErrors.INVALID_FLOWLET_NAME), flowlet);

        FlowletConnection.Type sourceType;
        String sourceName;
        if (fromStream != null) {
          sourceType = FlowletConnection.Type.STREAM;
          sourceName = fromStream;
        } else {
          sourceType = FlowletConnection.Type.FLOWLET;
          sourceName = fromFlowlet.getFlowletSpec().getName();
        }
        connections.add(new FlowletConnection(sourceType, sourceName, flowlet));
        return this;

      }

      @Override
      public FlowSpecification build() {
        return new DefaultFlowSpecification(name, description, flowlets, connections);
      }
    }

    private Builder() {
    }
  }
}
