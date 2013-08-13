/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Async;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.HashPartition;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.common.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator.Node;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.queue.QueueConsumerFactory;
import com.continuuity.internal.app.queue.QueueConsumerFactory.QueueInfo;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.RoundRobinQueueReader;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.MultiOutputSubmitter;
import com.continuuity.internal.app.runtime.OutputSubmitter;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.InstantiatorFactory;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class FlowletProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProgramRunner.class);

  private final SchemaGenerator schemaGenerator;
  private final DatumWriterFactory datumWriterFactory;
  private final DataFabricFacadeFactory txAgentSupplierFactory;
  private final QueueReaderFactory queueReaderFactory;
  private final MetricsCollectionService metricsCollectionService;

  private volatile List<QueueConsumerSupplier> queueConsumerSuppliers;

  @Inject
  public FlowletProgramRunner(SchemaGenerator schemaGenerator, DatumWriterFactory datumWriterFactory,
                              DataFabricFacadeFactory txAgentSupplierFactory,
                              QueueReaderFactory queueReaderFactory,
                              MetricsCollectionService metricsCollectionService) {
    this.schemaGenerator = schemaGenerator;
    this.datumWriterFactory = datumWriterFactory;
    this.txAgentSupplierFactory = txAgentSupplierFactory;
    this.queueReaderFactory = queueReaderFactory;
    this.queueConsumerSuppliers = ImmutableList.of();
    this.metricsCollectionService = metricsCollectionService;
  }

  @Inject(optional = true)
  void setLogWriter(LogWriter logWriter) {
    CAppender.logWriter = logWriter;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    BasicFlowletContext flowletContext = null;
    try {
      // Extract and verify parameters
      String flowletName = options.getName();

      int instanceId = Integer.parseInt(options.getArguments().getOption("instanceId", "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      int instanceCount = Integer.parseInt(options.getArguments().getOption("instances", "0"));
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      String runIdOption = options.getArguments().getOption("runId");
      Preconditions.checkNotNull(runIdOption, "Missing runId");
      RunId runId = RunIds.fromString(runIdOption);

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
      Class<? extends Flowlet> flowletClass = (Class<? extends Flowlet>) Class.forName(
                                                      flowletDef.getFlowletSpec().getClassName(),
                                                      true, classLoader);

      Preconditions.checkArgument(Flowlet.class.isAssignableFrom(flowletClass), "%s is not a Flowlet.", flowletClass);

      // Creates opex related objects
      DataFabricFacade txAgentSupplier = txAgentSupplierFactory.createDataFabricFacadeFactory(program);
      DataSetContext dataSetContext = txAgentSupplier.getDataSetContext();

      // Creates flowlet context
      flowletContext = new BasicFlowletContext(program, flowletName, instanceId,
                                               runId, instanceCount,
                                               DataSets.createDataSets(dataSetContext, flowletDef.getDatasets()),
                                               options.getUserArguments(),
                                               flowletDef.getFlowletSpec(),
                                               flowletClass.isAnnotationPresent(Async.class),
                                               metricsCollectionService);

      // Creates QueueSpecification
      Table<Node, String, Set<QueueSpecification>> queueSpecs =
        new SimpleQueueSpecificationGenerator(Id.Account.from(program.getAccountId()))
            .create(flowSpec);

      Flowlet flowlet = new InstantiatorFactory(false).get(TypeToken.of(flowletClass)).create();
      TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);

      // Inject DataSet, OutputEmitter, Metric fields
      flowletContext.injectFields(flowlet);
      OutputSubmitter outputSubmitter = injectFields(flowlet, flowletType,
                                                     outputEmitterFactory(flowletName, flowletContext,
                                                                          flowletContext.getQueueProducer(),
                                                                          queueSpecs));

      ImmutableList.Builder<QueueConsumerSupplier> queueConsumerSupplierBuilder = ImmutableList.builder();
      Collection<ProcessSpecification> processSpecs =
        createProcessSpecification(flowletType,
                                   processMethodFactory(flowlet, flowletContext,
                                                        txAgentSupplier,
                                                        outputSubmitter),
                                   processSpecificationFactory(program, queueReaderFactory,
                                                               flowletContext,
                                                               txAgentSupplier,
                                                               flowletName,
                                                               queueSpecs,
                                                               queueConsumerSupplierBuilder,
                                                               instanceCount,
                                                               createSchemaCache(program)
                                   ),
                                   Lists.<ProcessSpecification>newLinkedList());
      queueConsumerSuppliers = queueConsumerSupplierBuilder.build();

      FlowletProcessDriver driver = new FlowletProcessDriver(flowlet, flowletContext, processSpecs,
                                                             createCallback(flowlet, flowletDef.getFlowletSpec()));

      LOG.info("Starting flowlet: " + flowletContext);
      driver.start();
      LOG.info("Flowlet started: " + flowletContext);

      return programController(program.getProgramName(), flowletName, flowletContext, driver);

    } catch (Exception e) {
      // something went wrong before the flowlet even started. Make sure we release all resources (datasets, ...)
      // of the flowlet context.
      if (flowletContext != null) {
        flowletContext.close();
      }
      throw Throwables.propagate(e);
    }
  }

  private void changeInstanceCount(BasicFlowletContext flowletContext, int instanceCount) {
    flowletContext.setInstanceCount(instanceCount);
    for (QueueConsumerSupplier queueConsumerSupplier : queueConsumerSuppliers) {
      queueConsumerSupplier.updateInstanceCount(instanceCount);
    }
  }

  private ProgramController programController(String programName,
                                              final String flowletName,
                                              final BasicFlowletContext flowletContext,
                                              final FlowletProcessDriver driver) {
    return new FlowletProgramController(programName, flowletName, flowletContext, driver);
  }

  /**
   * Injects all {@link DataSet} and {@link OutputEmitter} fields.
   *
   * @return an {@link OutputSubmitter} that encapsulate all injected {@link OutputEmitter}
   *         that are {@link OutputSubmitter} as well.
   */
  private OutputSubmitter injectFields(Flowlet flowlet,
                                       TypeToken<? extends Flowlet> flowletType,
                                       OutputEmitterFactory outputEmitterFactory) {

    ImmutableList.Builder<OutputSubmitter> outputSubmitters = ImmutableList.builder();

    // Walk up the hierarchy of flowlet class.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject OutputEmitter fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject OutputEmitter
        if (OutputEmitter.class.equals(field.getType())) {
          TypeToken<?> outputType = TypeToken.of(((ParameterizedType) field.getGenericType())
                                                   .getActualTypeArguments()[0]);
          String outputName = field.isAnnotationPresent(Output.class) ?
            field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

          OutputEmitter<?> outputEmitter = outputEmitterFactory.create(outputName, outputType);
          setField(flowlet, field, outputEmitter);
          if (outputEmitter instanceof OutputSubmitter) {
            outputSubmitters.add((OutputSubmitter) outputEmitter);
          }
        }
      }
    }

    return new MultiOutputSubmitter(outputSubmitters.build());
  }

  /**
   * Creates all {@link ProcessSpecification} for the process methods of the flowlet class.
   *
   * @param flowletType Type of the flowlet class represented by {@link TypeToken}.
   * @param processMethodFactory A {@link ProcessMethodFactory} for creating {@link ProcessMethod}.
   * @param processSpecFactory A {@link ProcessSpecificationFactory} for creating {@link ProcessSpecification}.
   * @param result A {@link Collection} for storing newly created {@link ProcessSpecification}.
   * @return The same {@link Collection} as the {@code result} parameter.
   */
  private Collection<ProcessSpecification> createProcessSpecification(TypeToken<? extends Flowlet> flowletType,
                                                                      ProcessMethodFactory processMethodFactory,
                                                                      ProcessSpecificationFactory processSpecFactory,
                                                                      Collection<ProcessSpecification> result)
    throws NoSuchMethodException, OperationException {

    if (GeneratorFlowlet.class.isAssignableFrom(flowletType.getRawType())) {
      Method method = flowletType.getRawType().getMethod("generate");
      ProcessMethod generatorMethod = processMethodFactory.create(method);
      return ImmutableList.of(processSpecFactory.create(ImmutableSet.<String>of(),
                                                        Schema.of(Schema.Type.NULL), TypeToken.of(void.class),
                                                        generatorMethod, new QueueInfo()));
    }

    // Walk up the hierarchy of flowlet class to get all process methods
    // It needs to be traverse twice because process method needs to know all output emitters.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }
      // Extracts all process methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        ProcessInput processInputAnnotation = method.getAnnotation(ProcessInput.class);
        if (!method.getName().startsWith(FlowletDefinition.PROCESS_METHOD_PREFIX) && processInputAnnotation == null) {
          continue;
        }

        Set<String> inputNames;
        if (processInputAnnotation == null || processInputAnnotation.value().length == 0) {
          inputNames = ImmutableSet.of(FlowletDefinition.ANY_INPUT);
        } else {
          inputNames = ImmutableSet.copyOf(processInputAnnotation.value());
        }

        QueueInfo queueInfo;
        // Determine input queue partition type
        HashPartition hashPartition = method.getAnnotation(HashPartition.class);
        RoundRobin roundRobin = method.getAnnotation(RoundRobin.class);

        Preconditions.checkArgument(
          !(hashPartition != null && roundRobin != null),
          "%s: process() method can have either HashPartition or RoundRobin strategy to read input, not both.",
          method.getName());

        if (hashPartition != null) {
          Preconditions.checkArgument(!hashPartition.value().isEmpty(),
                                      "%s: Partition key cannot be empty when HashPartition used", method.getName());
          queueInfo = new QueueInfo(QueuePartitioner.PartitionerType.HASH, hashPartition.value());
        } else if (roundRobin != null) {
          queueInfo = new QueueInfo(QueuePartitioner.PartitionerType.ROUND_ROBIN);
        } else {
          queueInfo = new QueueInfo();
        }

        // Determine queue batch size, if any
        Batch batch = method.getAnnotation(Batch.class);
        if (batch != null) {
          queueInfo.setBatchMode(true);
          queueInfo.setBatchSize(batch.value());
        }

        try {
          // If batch mode then generate schema for Iterator's parameter type
          TypeToken<?> dataType = TypeToken.of(method.getGenericParameterTypes()[0]);
          if (batch != null) {
            Preconditions.checkArgument(dataType.getRawType().equals(Iterator.class), "" +
              "Batch mode without an Iterator as first parameter is not supported yet.");
            Preconditions.checkArgument(dataType.getType() instanceof ParameterizedType,
                                        "Iterator needs to be a ParameterizedType to extract type information");
            ParameterizedType pType = (ParameterizedType) dataType.getType();
            Preconditions.checkArgument(
              pType.getActualTypeArguments().length > 0,
              "Iterator does not define actual type parameters, cannot extract type information.");
            dataType = TypeToken.of(pType.getActualTypeArguments()[0]);
          }
          Schema schema = schemaGenerator.generate(dataType.getType());

          ProcessMethod processMethod = processMethodFactory.create(method);
          ProcessSpecification processSpec = processSpecFactory.create(inputNames, schema, dataType,
                                                                       processMethod, queueInfo);
          if (processSpec != null) {
            result.add(processSpec);
          }
        } catch (UnsupportedTypeException e) {
          throw Throwables.propagate(e);
        }
      }
    }
    Preconditions.checkArgument(!result.isEmpty(), "No process method found for " + flowletType);
    return result;
  }

  private Callback createCallback(Flowlet flowlet, FlowletSpecification flowletSpec) {
    if (flowlet instanceof Callback) {
      return (Callback) flowlet;
    }
    final FailurePolicy failurePolicy = flowletSpec.getFailurePolicy();
    return new Callback() {
      @Override
      public void onSuccess(Object input, InputContext inputContext) {
        // No-op
      }

      @Override
      public FailurePolicy onFailure(Object input, InputContext inputContext, FailureReason reason) {
        return failurePolicy;
      }
    };
  }

  private OutputEmitterFactory outputEmitterFactory(final String flowletName,
                                                    final BasicFlowletContext flowletContext,
                                                    final QueueProducer queueProducer,
                                                    final Table<Node, String, Set<QueueSpecification>> queueSpecs) {
    return new OutputEmitterFactory() {
      @Override
      public <T> OutputEmitter<T> create(String outputName, TypeToken<T> type) {
        try {
          Schema schema = schemaGenerator.generate(type.getType());
          Node flowlet = Node.flowlet(flowletName);
          for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.row(flowlet).values())) {
            if (queueSpec.getQueueName().getSimpleName().equals(outputName)
                && queueSpec.getOutputSchema().equals(schema)) {
              return new DatumOutputEmitter<T>(flowletContext, queueProducer, queueSpec.getQueueName(),
                                                    schema, datumWriterFactory.create(type, schema));
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
                                                    final BasicFlowletContext flowletContext,
                                                    final DataFabricFacade txAgentSupplier,
                                                    final OutputSubmitter outputSubmitter) {
    return new ProcessMethodFactory() {
      @Override
      public ProcessMethod create(Method method) {
        return ReflectionProcessMethod.create(flowlet, flowletContext, method,
                                              txAgentSupplier, outputSubmitter);


      }
    };
  }

  private ProcessSpecificationFactory
  processSpecificationFactory(final Program program,
                              final QueueReaderFactory queueReaderFactory,
                              final BasicFlowletContext flowletContext,
                              final DataFabricFacade dataFabricFacade,
                              final String flowletName,
                              final Table<Node, String, Set<QueueSpecification>> queueSpecs,
                              final ImmutableList.Builder<QueueConsumerSupplier> queueConsumerSupplierBuilder,
                              final int instanceCount, final SchemaCache schemaCache) {

    return new ProcessSpecificationFactory() {
      @Override
      public <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType,
                                         ProcessMethod method, QueueInfo queueInfo) {
        List<QueueReader> queueReaders = Lists.newLinkedList();

        for (Map.Entry<Node, Set<QueueSpecification>> entry : queueSpecs.column(flowletName).entrySet()) {
          for (QueueSpecification queueSpec : entry.getValue()) {
            final QueueName queueName = queueSpec.getQueueName();

            if (queueSpec.getInputSchema().equals(schema)
              && (inputNames.contains(queueName.getSimpleName())
              || inputNames.contains(FlowletDefinition.ANY_INPUT))) {

              int numGroups = (entry.getKey().getType() == FlowletConnection.Type.STREAM)
                                  ? -1
                                  : getNumGroups(Iterables.concat(queueSpecs.row(entry.getKey()).values()), queueName);


              QueueConsumerSupplier consumerSupplier = new QueueConsumerSupplier(
                dataFabricFacade.createQueueConsumerFactory(
                  flowletContext.getInstanceId(),
                  flowletContext.getGroupId(),
                  flowletContext.getMetricContext(),
                  queueName, queueInfo,
                  !flowletContext.isAsyncMode()
                ),
                instanceCount);
              queueConsumerSupplierBuilder.add(consumerSupplier);
              queueReaders.add(queueReaderFactory.create(program, queueName, consumerSupplier, numGroups));
            }
          }
        }

        // If inputs is needed but there is no available input queue, return null
        if (!inputNames.isEmpty() && queueReaders.isEmpty()) {
          return null;
        }
        return new ProcessSpecification<T>(new RoundRobinQueueReader(queueReaders),
                                        createInputDatumDecoder(dataType, schema, schemaCache),
                                        method);
      }
    };
  }

  private <T> Function<ByteBuffer, T> createInputDatumDecoder(final TypeToken<T> dataType, final Schema schema,
                                                              final SchemaCache schemaCache) {
    final ReflectionDatumReader<T> datumReader = new ReflectionDatumReader<T>(schema, dataType);
    final ByteBufferInputStream byteBufferInput = new ByteBufferInputStream(null);
    final BinaryDecoder decoder = new BinaryDecoder(byteBufferInput);

    return new Function<ByteBuffer, T>() {
      @Nullable
      @Override
      public T apply(ByteBuffer input) {
        byteBufferInput.reset(input);
        try {
          final Schema sourceSchema = schemaCache.get(input);
          Preconditions.checkNotNull(sourceSchema, "Fail to find source schema.");
          return datumReader.read(decoder, sourceSchema);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
          .add("dataType", dataType)
          .add("schema", schema)
          .toString();
      }
    };
  }

  private static final class QueueConsumerSupplier implements Supplier<QueueConsumer> {
    private final QueueConsumerFactory queueConsumerFactory;
    private volatile QueueConsumer consumer;

    public QueueConsumerSupplier(QueueConsumerFactory queueConsumerFactory, int groupSize) {
      this.queueConsumerFactory = queueConsumerFactory;
      this.consumer = queueConsumerFactory.create(groupSize);
    }

    public void updateInstanceCount(int groupSize) {
      consumer = queueConsumerFactory.create(groupSize);
    }

    @Override
    public QueueConsumer get() {
      return consumer;
    }

  }

  private int getNumGroups(Iterable<QueueSpecification> queueSpecs, QueueName queueName) {
    int numGroups = 0;
    for (QueueSpecification queueSpec : queueSpecs) {
      if (queueName.equals(queueSpec.getQueueName())) {
        numGroups++;
      }
    }
    return numGroups;
  }

  private void setField(Flowlet flowlet, Field field, Object value) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      field.set(flowlet, value);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
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
    <T> OutputEmitter<T> create(String outputName, TypeToken<T> type);
  }

  private static interface ProcessMethodFactory {
    ProcessMethod create(Method method);
  }

  private static interface ProcessSpecificationFactory {
    /**
     * Returns a {@link ProcessSpecification} for invoking the given process method. {@code null} is returned if
     * no input is available for the given method.
     */
    <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType, ProcessMethod method,
                                QueueInfo queueInfo) throws OperationException;
  }

  private class FlowletProgramController extends AbstractProgramController {
    private final BasicFlowletContext flowletContext;
    private final FlowletProcessDriver driver;

    FlowletProgramController(String programName, String flowletName,
                             BasicFlowletContext flowletContext, FlowletProcessDriver driver) {
      super(programName + ":" + flowletName, flowletContext.getRunId());
      this.flowletContext = flowletContext;
      this.driver = driver;
      started();
    }

    @Override
    protected void doSuspend() throws Exception {
      LOG.info("Suspending flowlet: " + flowletContext);
      driver.suspend();
      LOG.info("Flowlet suspended: " + flowletContext);
    }

    @Override
    protected void doResume() throws Exception {
      LOG.info("Resuming flowlet: " + flowletContext);
      driver.resume();
      LOG.info("Flowlet resumed: " + flowletContext);
    }

    @Override
    protected void doStop() throws Exception {
      LOG.info("Stopping flowlet: " + flowletContext);
      driver.stopAndWait();
      LOG.info("Flowlet stopped: " + flowletContext);
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      Preconditions.checkState(getState() == State.SUSPENDED,
                               "Cannot change instance count when flowlet is running.");
      if (!"instances".equals(name) || !(value instanceof Integer)) {
        return;
      }
      int instances = (Integer) value;
      LOG.info("Change flowlet instance count: " + flowletContext + ", new count is " + instances);
      changeInstanceCount(flowletContext, instances);
      LOG.info("Flowlet instance count changed: " + flowletContext + ", new count is " + instances);
    }
  }
}
