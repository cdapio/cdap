package com.continuuity.api.flow;

import com.continuuity.api.annotation.DataSet;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
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

  private static final String PROCESS_METHOD_PREFIX = "process";
  private static final String DEFAULT_OUTPUT = "out";
  private static final String ANY_INPUT = "";

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
   */
  private final List<FlowletConnection> connections;

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
                            List<FlowletConnection> connections) {
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
    private final Map<String, TypeToken<?>> inputs;
    private final Map<String, TypeToken<?>> outputs;

    private FlowletDefinition(Flowlet flowlet, int instances, ResourceSpecification resourceSpec) {
      this.flowletSpec = flowlet.configure();
      this.instances = instances;
      this.resourceSpec = resourceSpec;

      Map<String, TypeToken<?>> inputs = Maps.newHashMap();
      Map<String, TypeToken<?>> outputs = Maps.newHashMap();
      inspectFlowlet(flowlet.getClass(), inputs, outputs);

      this.inputs = Collections.unmodifiableMap(inputs);
      this.outputs = Collections.unmodifiableMap(outputs);
    }

    public int getInstances() {
      return instances;
    }

    public FlowletSpecification getFlowletSpec() {
      return flowletSpec;
    }

    public ResourceSpecification getResourceSpec() {
      return resourceSpec;
    }

    public Map<String, TypeToken<?>> getInputs() {
      return inputs;
    }

    public Map<String, TypeToken<?>> getOutputs() {
      return outputs;
    }

    private void inspectFlowlet(Class<?> flowletClass,
                                Map<String, TypeToken<?>> inputs,
                                Map<String, TypeToken<?>> outputs) {
      TypeToken<?> flowletType = TypeToken.of(flowletClass);

      // Walk up the hierarchy of flowlet class.
      for (TypeToken<?> type : flowletType.getTypes().classes()) {

        // Grab all the OutputEmitter fields
        for (Field field : type.getRawType().getDeclaredFields()) {
          if (!OutputEmitter.class.equals(field.getType())) {
            continue;
          }

          Type emitterType = field.getGenericType();
          Preconditions.checkArgument(emitterType instanceof ParameterizedType,
                                      "Type info missing from OutputEmitter; class: %s; field: %s.", type, field);

          // Extract the Output type from the first type argument of OutputEmitter
          TypeToken<?> outputType = TypeToken.of(((ParameterizedType) emitterType).getActualTypeArguments()[0]);
          String outputName = field.isAnnotationPresent(DataSet.class) ?
                                  field.getAnnotation(DataSet.class).value() : DEFAULT_OUTPUT;
          if (!outputs.containsKey(outputName)) {
            outputs.put(outputName, outputType);
          }
        }

        // Grab all process methods
        for (Method method : type.getRawType().getDeclaredMethods()) {
          Process processAnnotation = method.getAnnotation(Process.class);
          if (!method.getName().startsWith(PROCESS_METHOD_PREFIX) && processAnnotation == null) {
            continue;
          }

          Type[] methodParams = method.getGenericParameterTypes();
          Preconditions.checkArgument(methodParams.length > 0,
                                      "Type parameter missing from process method; class: %s, method: %s", type, method);

          // Extract the Input type from the first parameter of the process method
          TypeToken<?> inputType = type.resolveType(methodParams[0]);
          if (processAnnotation == null || processAnnotation.value().length == 0) {
            inputs.put(ANY_INPUT, inputType);
          } else {
            for (String name : processAnnotation.value()) {
              inputs.put(name, inputType);
            }
          }
        }
      }
    }
  }


  public static final class FlowletConnection {

    public enum SourceType {
      STREAM,
      FLOWLET
    }

    private final SourceType sourceType;
    private final String sourceName;
    private final String targetName;
    private final String sourceOutput;
    private final String targetInput;

    public FlowletConnection(SourceType sourceType, String sourceName, String targetName, String sourceOutput, String targetInput) {
      this.sourceType = sourceType;
      this.sourceName = sourceName;
      this.targetName = targetName;
      this.sourceOutput = sourceOutput;
      this.targetInput = targetInput;
    }

    public SourceType getSourceType() {
      return sourceType;
    }

    public String getSourceName() {
      return sourceName;
    }

    public String getTargetName() {
      return targetName;
    }

    public String getSourceOutput() {
      return sourceOutput;
    }

    public String getTargetInput() {
      return targetInput;
    }
  }

  public static final class Builder {

    private String name;
    private String description;
    private final Set<String> streams = Sets.newHashSet();
    private final Map<String, FlowletDefinition> flowlets = Maps.newHashMap();
    private final List<FlowletConnection> connections = Lists.newArrayList();

    public DescriptionSetter setName(String name) {
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.name = name;
      return new DescriptionSetter();
    }

    public final class DescriptionSetter {
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
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
            String flowletName = flowletDef.getFlowletSpec().getName();

            Preconditions.checkArgument(!flowlets.containsKey(flowletName), "Flowlet %s already defined", flowletName);

            flowlets.put(flowletName, flowletDef);
            return moreFlowlet;
          }
        });
      }

      public ConnectFrom connect() {
        // Collect all input streams names
        for (FlowletDefinition flowletDef : flowlets.values()) {
          for (Map.Entry<String, TypeToken<?>> inputEntry : flowletDef.getInputs().entrySet()) {
            if (StreamEvent.class.equals(inputEntry.getValue().getRawType())) {
              streams.add(inputEntry.getKey());
            }
          }
        }

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

      private String fromStream;
      private FlowletDefinition fromFlowlet;

      @Override
      public ConnectTo from(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        fromFlowlet = flowlets.get(flowletName);
        fromStream = null;

        return this;
      }

      @Override
      public ConnectTo from(Stream stream) {
        Preconditions.checkArgument(stream != null, "Stream cannot be null");
        StreamSpecification streamSpec = stream.configure();
        Preconditions.checkArgument(streams.contains(streamSpec.getName()) || streams.contains(ANY_INPUT),
                                    "Stream %s is not accepted by any configured flowlet.", streamSpec.getName());

        fromFlowlet = null;
        fromStream = streamSpec.getName();

        return this;
      }

      @Override
      public MoreConnect to(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        FlowletDefinition flowletDef = flowlets.get(flowletName);

        if (fromStream != null) {
          TypeToken<?> type = flowletDef.getInputs().get(fromStream);
          String targetInput = fromStream;
          if (type == null) {
            type = flowletDef.getInputs().get(ANY_INPUT);
            targetInput = ANY_INPUT;
          }
          Preconditions.checkArgument(StreamEvent.class.equals(type.getRawType()), "Cannot cannot stream %s to flowlet %s", fromStream, flowletName);
          connections.add(new FlowletConnection(FlowletConnection.SourceType.STREAM, "", flowletName, fromStream, targetInput));

        } else {
          // TODO: Check if the output types of fromFlowlet is compatible with input types of toFlowlet
          // Need supports from the serialization library to implement it.

//          connections.add(new FlowletConnection(FlowletConnection.SourceType.FLOWLET, fromFlowlet.getFlowletSpec().getName(), flowletName, ))
        }

        return this;
      }

      @Override
      public FlowSpecification build() {
        return new FlowSpecification(name, description, Collections.unmodifiableMap(flowlets), Collections.unmodifiableList(connections));
      }
    }

    private Builder() {
    }
  }
}
