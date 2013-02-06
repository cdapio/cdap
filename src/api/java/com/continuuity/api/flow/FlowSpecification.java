package com.continuuity.api.flow;

import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 * This class provides specification of a Flow. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link #builder()} method.
 *
 * <pre>
 * {@code
 * FlowSpecification flowSpecification =
 *      FlowSpecification.builder()
 *        .setName("tokenCount")
 *        .setDescription("Token counting flow")
 *        .withDataset().add("token")
 *        .withStream().add("text")
 *        .withFlowlet().add("source", StreamSource.class, 1).apply()
 *                      .add("tokenizer", Tokenizer.class, 1).setCpu(1).setMemoryInMB(100).apply()
 *                      .add("count", CountByField.class, 1).apply()
 *        .withInput().add("text", "source")
 *        .withConnection().add("source", "tokenizer")
 *                         .add("tokenizer", "count")
 *        .build();
 * }
 * </pre>
 */
public final class FlowSpecification {

  /**
   * Name of the flow
   */
  private final String name;

  /**
   * Description about the flow
   */
  private final String description;

  /**
   * Set of flowlets that constitute the flow. Map from flowlet id to {@link FlowletDefinition}
   */
  private final Map<String, FlowletDefinition> flowlets;

  /**
   * Stores flowlet connections.
   *
   * Row -> Source flowlet
   * Column -> Destination flowlet
   * Cell -> (Source stream id, Destination stream id)
   */
  private final Table<String, String, ImmutablePair<String, String>> connections;

  /**
   * Creates a {@link Builder} for building instance of this class.
   *
   * @return a new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Private constructor, only called by {@link Builder}.
   */
  private FlowSpecification(String name, String description,
                            Map<String, FlowletDefinition> flowlets,
                            Table<String, String, ImmutablePair<String, String>> connections) {
    this.name = name;
    this.description = description;
    this.flowlets = flowlets;
    this.connections = connections;
  }

  /**
   * @return Name of the flow.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the flow.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Immutable Map from flowlet name to {@link FlowletDefinition}.
   */
  public Map<String, FlowletDefinition> getFlowlets() {
    return flowlets;
  }


//  public Table<String, String, ImmutablePair<String, String>> getConnections() {
//    return connections;
//  }

  public static final class FlowletDefinition {
    private final FlowletSpecification flowletSpec;
    private final int instances;
    private final ResourceSpecification resourceSpec;
    private final Map<String, TypeToken> inputs;
    private final Map<String, TypeToken> outputs;

    private FlowletDefinition(Flowlet flowlet, int instances, ResourceSpecification resourceSpec) {
      this.flowletSpec = flowlet.configure();
      this.instances = instances;
      this.resourceSpec = resourceSpec;

      this.inputs = Maps.newHashMap();
      this.outputs = Maps.newHashMap();
      inspectFlowlet(flowlet.getClass());
    }

    /**
     * @return Number of instances configured for this flowlet.
     */
    public int getInstances() {
      return instances;
    }

    /**
     * @return Specification of Flowlet
     */
    public FlowletSpecification getFlowletSpec() {
      return flowletSpec;
    }

    /**
     * @return Specification for resource.
     */
    public ResourceSpecification getResourceSpec() {
      return resourceSpec;
    }

    private void inspectFlowlet(Class<?> flowletClass) {
      // TODO
//      // Grab all the OutputEmitter
//      for (Field field : flowletClass.getDeclaredFields()) {
//        TypeToken<?> fieldType = TypeToken.of(field.getGenericType());
//        if (OutputEmitter.class.isAssignableFrom(fieldType.getRawType())) {
////          fieldType.resolveType()
//        }
//      }
    }
  }

  public static final class Builder {
    private String name;
    private String description;
    private final Set<String> streams = Sets.newHashSet();
    private final Map<String, FlowletDefinition> flowlets = Maps.newHashMap();
    private final Table<String, String, ImmutablePair<String, String>> connections = HashBasedTable.create();

    public DescriptionSetter setName(String name) {
      this.name = name;
      return new DescriptionSetter();
    }

    public final class DescriptionSetter {
      public AfterDescription setDescription(String description) {
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    public final class AfterDescription {
      public FlowletAdder withFlowlet() {
        return new MoreFlowlet();
      }
    }

    public interface FlowletAdder {
      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet);

      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet, int instances);
    }

    public final class MoreFlowlet implements FlowletAdder {

      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet) {
        return add(flowlet, 1);
      }

      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(final Flowlet flowlet, final int instances) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        Preconditions.checkArgument(instances > 0, "Number of instances must be > 0");

        final MoreFlowlet moreFlowlet = this;
        return ResourceSpecification.<MoreFlowlet>builder(new Function<ResourceSpecification, MoreFlowlet>() {
          @Override
          public MoreFlowlet apply(ResourceSpecification resourceSpec) {
            FlowletDefinition flowletDef = new FlowletDefinition(flowlet, instances, resourceSpec);
            flowlets.put(flowletDef.getFlowletSpec().getName(), flowletDef);
            return moreFlowlet;
          }
        });
      }

      public ConnectFrom connect() {
        return new Connector();
      }
    }

    public interface ConnectFrom {
      ConnectTo from(Flowlet flowlet);

      ConnectTo from(Stream stream);
    }

    public interface ConnectTo {
      MoreConnect to(Flowlet flowlet);
    }

    public interface MoreConnect extends ConnectFrom {
      FlowSpecification build();
    }

    public final class Connector implements ConnectFrom, ConnectTo, MoreConnect {

      @Override
      public ConnectTo from(Flowlet flowlet) {
        return this;
      }

      @Override
      public ConnectTo from(Stream stream) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public MoreConnect to(Flowlet flowlet) {
        return this;
      }

      @Override
      public FlowSpecification build() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
      }
    }

    private Builder() {
    }
  }
}
