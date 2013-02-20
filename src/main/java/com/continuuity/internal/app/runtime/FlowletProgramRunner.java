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
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.runtime.Controller;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class FlowletProgramRunner implements ProgramRunner {

  private final OperationExecutor opex;

  @Inject
  public FlowletProgramRunner(OperationExecutor opex) {
    this.opex = opex;
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
//      Table<String,String,Set<QueueSpecification>> queueSpecs =
//        new SimpleQueueSpecificationGenerator(Id.Account.from(program.getAccountId()))
//            .create(flowSpec);
//
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

  private FlowletProcessDriver instantiateFlowlet(Class<? extends Flowlet> flowletClass,
                                                  FlowletContext flowletContext,
                                                  OutputEmitterFactory outputEmitterFactory,
                                                  SchemaCache schemaCache,
                                                  FlowletDefinition flowletDef,
                                                  FlowSpecification flowSpec,
                                                  String flowletName,
                                                  TransactionAgentSupplier txAgentSupplier) throws Exception {

    TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);
    Flowlet flowlet = flowletClass.newInstance();
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

          OutputEmitter<?> outputEmitter = outputEmitterFactory.create(outputType);
          setField(flowlet, field, outputEmitter);
          if (outputEmitter instanceof OutputSubmitter) {
            outputSubmitters.add((OutputSubmitter)outputEmitter);
          }
        }
      }
    }

    OutputSubmitter outputSubmitter = new MultiOutputSubmitter(outputSubmitters.build());

    // Walk up the hierarchy of flowlet class again to get all process methods
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

        List<String> inputNames;
        if (processAnnotation == null || processAnnotation.value().length == 0) {
          inputNames = ImmutableList.of(FlowletDefinition.ANY_INPUT);
        } else {
          inputNames = ImmutableList.copyOf(processAnnotation.value());
        }

        // TODO: Find out the queue name by input name
        // TODO: Create method invoker

      }
    }
    return null;
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
                                                    final Table<String, String, Set<QueueSpecification>> queueSpecs) {
    final SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();

    return new OutputEmitterFactory() {
      @Override
      public OutputEmitter<?> create(TypeToken<?> type) {
        try {
          Schema schema = schemaGenerator.generate(type.getType());

          for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.row(flowletName).values())) {
            if (queueSpec.getInputSchema().equals(schema)) {
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
    OutputEmitter<?> create(TypeToken<?> type);
  }
}
