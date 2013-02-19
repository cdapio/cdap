/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.internal.api.flow.DefaultFlowSpecification;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * This class provides specification of a Flow. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link Builder#with()} method.
 *
 * <pre>
 * {@code
 * FlowSpecification flowSpecification =
 *      FlowSpecification.Builder.with()
 *        .setName("tokenCount")
 *        .setDescription("Token counting flow")
 *        .withDataset().add("token")
 *        .withStream().add("text")
 *        .withFlowlets().add("source", StreamSource.class, 1).apply()
 *                      .add("tokenizer", Tokenizer.class, 1).setCpu(1).setMemoryInMB(100).apply()
 *                      .add("count", CountByField.class, 1).apply()
 *        .withInput().add("text", "source")
 *        .withConnection().add("source", "tokenizer")
 *                         .add("tokenizer", "count")
 *        .build();
 * }
 * </pre>
 */
public interface FlowSpecification {

  /**
   * @return Class name of the {@link Flow}.
   */
  String getClassName();

  /**
   * @return Name of the {@link Flow}.
   */
  String getName();

  /**
   * @return Description of the flow.
   */
  String getDescription();

  /**
   * @return Immutable Map from flowlet name to {@link FlowletDefinition}.
   */
  Map<String, FlowletDefinition> getFlowlets();

  /**
   * @return Immutable list of {@link FlowletConnection}.
   */
  List<FlowletConnection> getConnections();

  /**
   * Defines builder for building connections or topology for a flow.
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
       * Sets the name of the Flow
       * @param name of the flow.
       * @return An instance of {@link DescriptionSetter}
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Defines a class for defining the actual description.
     */
    public final class DescriptionSetter {
      /**
       * Sets the description for the flow.
       * @param description of the flow.
       * @return A instance of {@link AfterDescription}
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
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
       * @param flowlet to be added to flow.
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet);

      /**
       * Add a flowlet to flow with minimum number of instance to begin with.
       * @param flowlet to be added to flow.
       * @param instances of flowlet
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet, int instances);
    }

    /**
     * This class allows more flowlets to be defined. This is part of a controlled builder.
     */
    public final class MoreFlowlet implements FlowletAdder {

      /**
       * Add a flowlet to the flow.
       * @param flowlet to be added to flow.
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet) {
        return add(flowlet, 1);
      }

      /**
       * Adds a flowlet to flow with minimum number of instance to begin with.
       * @param flowlet to be added to flow.
       * @param instances of flowlet
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(final Flowlet flowlet, final int instances) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        Preconditions.checkArgument(instances > 0, "Number of instances must be > 0");

        final MoreFlowlet moreFlowlet = this;
        return ResourceSpecification.<MoreFlowlet>builder(new Function<ResourceSpecification, MoreFlowlet>() {
          @Override
          public MoreFlowlet apply(ResourceSpecification resourceSpec) {
            FlowletDefinition flowletDef = new FlowletDefinition(flowlet, instances, resourceSpec);
            String flowletName = flowletDef.getFlowletSpec().getName();
            Preconditions.checkArgument(!flowlets.containsKey(flowletName), "Flowlet %s already defined", flowletName);
            flowlets.put(flowletName, flowletDef);
            return moreFlowlet;
          }
        });
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
       * Defines the flowlet that is at run of the connection.
       * @param flowlet that is run of connection.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Flowlet flowlet);

      /**
       * Defines the stream that the connection is reading from.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Stream stream);
    }

    /**
     * Class defining the connect to interface for a connection.
     */
    public interface ConnectTo {
      /**
       * Defines the flowlet that the connection is connecting to.
       * @param flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      MoreConnect to(Flowlet flowlet);
    }

    /**
     * Interface defines the building of FlowSpecification.
     */
    public interface MoreConnect extends ConnectFrom {
      /**
       * Constructs a {@link FlowSpecification}
       * @return An instance of {@link FlowSpecification}
       */
      FlowSpecification build();
    }

    /**
     * Class defines the connection between two flowlets.
     */
    public final class Connector implements ConnectFrom, ConnectTo, MoreConnect {

      private String fromStream;
      private FlowletDefinition fromFlowlet;

      /**
       * Defines the flowlet that is at run of the connection.
       * @param flowlet that is run of connection.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      @Override
      public ConnectTo from(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        fromFlowlet = flowlets.get(flowletName);
        fromStream = null;

        return this;
      }

      /**
       * Defines the stream that the connection is reading from.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      @Override
      public ConnectTo from(Stream stream) {
        Preconditions.checkArgument(stream != null, "Stream cannot be null");
        StreamSpecification streamSpec = stream.configure();

        fromFlowlet = null;
        fromStream = streamSpec.getName();

        return this;
      }

      /**
       * Defines the flowlet that the connection is connecting to.
       * @param flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      @Override
      public MoreConnect to(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        FlowletConnection.SourceType sourceType;
        String sourceName;
        if (fromStream != null) {
          sourceType = FlowletConnection.SourceType.STREAM;
          sourceName = fromStream;
        } else {
          sourceType = FlowletConnection.SourceType.FLOWLET;
          sourceName = fromFlowlet.getFlowletSpec().getName();
        }
        connections.add(new FlowletConnection(sourceType, sourceName, flowletName));
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
