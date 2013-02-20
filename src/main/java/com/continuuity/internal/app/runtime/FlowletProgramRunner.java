/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.runtime.Controller;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.queue.RoundRobinQueueReader;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.queue.SingleQueueReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class FlowletProgramRunner implements ProgramRunner {

  private final OperationExecutor opex;
  private final SchemaGenerator schemaGenerator;

  @Inject
  public FlowletProgramRunner(OperationExecutor opex, SchemaGenerator schemaGenerator) {
    this.opex = opex;
    this.schemaGenerator = schemaGenerator;
  }

  @Override
  public Controller run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      Preconditions.checkArgument(program.getProcessorType() == Type.FLOW, "Supported process type");

      String flowletName = options.getName();
      int instanceId = Integer.parseInt(options.getArguments().getOption("instanceId", "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getProcessorType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

      String processorName = program.getProgramName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      FlowSpecification flowSpec = appSpec.getFlows().get(processorName);
      FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletName);

      Preconditions.checkNotNull(flowletDef, "Definition missing for flowlet \"%s\"", flowletName);
      ClassLoader classLoader = program.getMainClass().getClassLoader();
      Class<? extends Flowlet> flowletClass = (Class<? extends Flowlet>)
                                                  Class.forName(flowletDef.getFlowletSpec().getClassName(),
                                                                true, classLoader);

      Preconditions.checkArgument(Flowlet.class.isAssignableFrom(flowletClass), "%s is not a Flowlet.", flowletClass);

      // Creates opex related objects
      OperationContext opCtx = new OperationContext(program.getAccountId(), program.getApplicationId());
      TransactionProxy transactionProxy = new TransactionProxy();
      TransactionAgentSupplier txAgentSupplier = new SmartTransactionAgentSupplier(opex, opCtx, transactionProxy);
      DataFabric dataFabric = new DataFabricImpl(opex, opCtx);
      DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric, transactionProxy, classLoader);
      dataSetInstantiator.setDataSets(ImmutableList.copyOf(appSpec.getDataSets().values()));

      // Creates flowlet context
      BasicFlowletContext flowletContext = new BasicFlowletContext(program, flowletName, instanceId,
                                                                   createDataSets(dataSetInstantiator, flowletDef));

      // Creates QueueSpecification
      Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs =
        new SimpleQueueSpecificationGenerator(Id.Account.from(program.getAccountId()))
            .create(flowSpec);

      // Create Logging context
      LoggingContext loggingContext = new FlowletLoggingContext(program.getAccountId(),
                                                                program.getApplicationId(),
                                                                program.getProgramName(),
                                                                flowletName);

      Flowlet flowlet = flowletClass.newInstance();
      TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);

      OutputSubmitter outputSubmitter = injectFields(flowlet, flowletType, flowletContext,
                                                     outputEmitterFactory(flowletName,
                                                                          flowletContext.getQueueProducer(),
                                                                          queueSpecs));


//      createProcessSpecification()
//      new FlowletProcessDriver(flowlet, flowletContext, loggingContext, processSpecs, createCallback(flowlet, flowletDef.getFlowletSpec()));


//      final FlowletProcessDriver driver = instantiateFlowlet(flowletClass,
//                                                             flowletContext,
//                                                             outputEmitterFactory(flowletContext.getQueueProducer(),
//                                                                                  queueSpecs));

//      driver.start();
//      return new Controller() {
//        @Override public void suspend() { driver.suspend(); }
//        @Override public void resume() { driver.resume(); }
//        @Override public void stop() { driver.stopAndWait(); }
//      };
      return null;

    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Injects all {@link DataSet} and {@link OutputEmitter} fields.
   *
   * @return an {@link OutputSubmitter} that encapsulate all injected {@link OutputEmitter}
   *         that are {@link OutputSubmitter} as well.
   */
  private OutputSubmitter injectFields(Flowlet flowlet,
                                       TypeToken<? extends Flowlet> flowletType,
                                       FlowletContext flowletContext,
                                       OutputEmitterFactory outputEmitterFactory) throws IllegalAccessException {

    ImmutableList.Builder<OutputSubmitter> outputSubmitters = ImmutableList.builder();

    // Walk up the hierarchy of flowlet class.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Emitter fields
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset != null && !dataset.value().isEmpty()) {
            setField(flowlet, field, flowletContext.getDataSet(dataset.value()));
          }
          continue;
        }
        // Inject OutputEmitter
        if (OutputEmitter.class.equals(field.getType())) {
          TypeToken<?> outputType = TypeToken.of(((ParameterizedType)field.getGenericType())
                                                   .getActualTypeArguments()[0]);
          String outputName = field.isAnnotationPresent(Output.class) ?
            field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

          OutputEmitter<?> outputEmitter = outputEmitterFactory.create(outputName, outputType);
          setField(flowlet, field, outputEmitter);
          if (outputEmitter instanceof OutputSubmitter) {
            outputSubmitters.add((OutputSubmitter)outputEmitter);
          }
        }
      }
    }

    return new MultiOutputSubmitter(outputSubmitters.build());
  }

  private Collection<ProcessSpecification> createProcessSpecification(TypeToken<? extends Flowlet> flowletType,
                                                                      ProcessMethodFactory processMethodFactory,
                                                                      ProcessSpecificationFactory processSpecFactory,
                                                                      Collection<ProcessSpecification> result)
                                                                      throws UnsupportedTypeException {

    // Walk up the hierarchy of flowlet class to get all process methods
    // It needs to be traverse twice because process method needs to know all output emitters.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }
      // Extracts all process methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        Process processAnnotation = method.getAnnotation(Process.class);
        if (!method.getName().startsWith(FlowletDefinition.PROCESS_METHOD_PREFIX) && processAnnotation == null) {
          continue;
        }

        Set<String> inputNames;
        if (processAnnotation == null || processAnnotation.value().length == 0) {
          inputNames = ImmutableSet.of(FlowletDefinition.ANY_INPUT);
        } else {
          inputNames = ImmutableSet.copyOf(processAnnotation.value());
        }

        TypeToken<?> dataType = TypeToken.of(method.getGenericParameterTypes()[0]);
        Schema schema = schemaGenerator.generate(dataType.getType());

        ProcessMethod processMethod = processMethodFactory.create(method, dataType, schema);
        result.add(processSpecFactory.create(inputNames, schema, processMethod));
      }
    }
    return result;
  }

  private Callback createCallback(Flowlet flowlet, FlowletSpecification flowletSpec) {
    if (flowlet instanceof Callback) {
      return (Callback)flowlet;
    }
    final FailurePolicy failurePolicy = flowletSpec.getFailurePolicy();
    return new Callback() {
      @Override
      public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
        // No-op
      }

      @Override
      public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason
        reason) {
        return failurePolicy;
      }
    };
  }

  private Map<String, DataSet> createDataSets(DataSetContext dataSetContext,
                                              FlowletDefinition flowletDef) {

    ImmutableMap.Builder<String, DataSet> builder = ImmutableMap.builder();

    for (String dataSetName : flowletDef.getDatasets()) {
      builder.put(dataSetName, dataSetContext.getDataSet(dataSetName));
    }

    return builder.build();
  }

  private OutputEmitterFactory outputEmitterFactory(final String flowletName,
                                                    final QueueProducer queueProducer,
                                                    final Table<QueueSpecificationGenerator.Node,
                                                                String,
                                                                Set<QueueSpecification>> queueSpecs) {
    return new OutputEmitterFactory() {
      @Override
      public OutputEmitter<?> create(String outputName, TypeToken<?> type) {
        try {
          Schema schema = schemaGenerator.generate(type.getType());

          QueueSpecificationGenerator.Node flowlet = QueueSpecificationGenerator.Node.flowlet(flowletName);
          for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.row(flowlet).values())) {
            if (queueSpec.getQueueName().getSimpleName().equals(outputName)
                && queueSpec.getOutputSchema().equals(schema)) {
              return new ReflectionOutputEmitter(queueProducer, queueSpec.getQueueName(), schema);
            }
          }

          throw new IllegalArgumentException(String.format("No queue specification found for %s, %s",
                                                           flowletName, type));

        } catch (UnsupportedTypeException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private ProcessMethodFactory processMethodFactory(final Flowlet flowlet,
                                                    final SchemaCache schemaCache,
                                                    final TransactionAgentSupplier txAgentSupplier,
                                                    final OutputSubmitter outputSubmitter) {
    return new ProcessMethodFactory() {
      @Override
      public ProcessMethod create(Method method, TypeToken<?> dataType, Schema schema) {
        return ReflectionProcessMethod.create(flowlet, method, dataType, schema,
                                              schemaCache, txAgentSupplier, outputSubmitter);


      }
    };
  }

  private ProcessSpecificationFactory processSpecificationFactory(final OperationExecutor opex,
                                                                  final OperationContext operationCtx,
                                                                  final QueueConsumer queueConsumer,
                                                                  final String flowletName,
                                                                  final Table<QueueSpecificationGenerator.Node,
                                                                              String,
                                                                              Set<QueueSpecification>> queueSpecs) {
    return new ProcessSpecificationFactory() {
      @Override
      public ProcessSpecification create(Set<String> inputNames, Schema schema, ProcessMethod method) {
        List<QueueReader> queueReaders = Lists.newLinkedList();

        for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletName).values())) {
          QueueName queueName = queueSpec.getQueueName();
          if (queueSpec.getInputSchema().equals(schema)
              && (inputNames.contains(queueName.getSimpleName())
                  || inputNames.contains(FlowletDefinition.ANY_INPUT))) {

            queueReaders.add(new SingleQueueReader(opex, operationCtx, queueName, queueConsumer));
          }
        }

        Preconditions.checkArgument(!queueReaders.isEmpty(), "No queue reader found for %s %s", flowletName, schema);
        return new ProcessSpecification(new RoundRobinQueueReader(queueReaders), method);
      }
    };
  }

  private void setField(Flowlet flowlet, Field field, Object value) throws IllegalAccessException {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    field.set(flowlet, value);
  }

  private SchemaCache createSchemaCache(Program program) throws ClassNotFoundException {
    ImmutableSet.Builder<Schema> schemas = ImmutableSet.builder();

    for (FlowSpecification flowSpec : program.getSpecification().getFlows().values()) {
      for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
        schemas.addAll(Iterables.concat(flowletDef.getInputs().values()));
        schemas.addAll(Iterables.concat(flowletDef.getOutputs().values()));
      }
    }

    return new SchemaCache(schemas.build(), program.getMainClass().getClassLoader());
  }

  private static interface OutputEmitterFactory {
    OutputEmitter<?> create(String outputName, TypeToken<?> type);
  }

  private static interface ProcessMethodFactory {
    ProcessMethod create(Method method, TypeToken<?> dataType, Schema schema);
  }

  private static interface ProcessSpecificationFactory {
    ProcessSpecification create(Set<String> inputNames, Schema schema, ProcessMethod method);
  }
}