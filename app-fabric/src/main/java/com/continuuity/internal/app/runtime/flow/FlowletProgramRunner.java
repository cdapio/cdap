/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.HashPartition;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator.Node;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.RoundRobinQueueReader;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.DataSetFieldSetter;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
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
  private final DataFabricFacadeFactory dataFabricFacadeFactory;
  private final QueueReaderFactory queueReaderFactory;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public FlowletProgramRunner(SchemaGenerator schemaGenerator, DatumWriterFactory datumWriterFactory,
                              DataFabricFacadeFactory dataFabricFacadeFactory,
                              QueueReaderFactory queueReaderFactory,
                              MetricsCollectionService metricsCollectionService) {
    this.schemaGenerator = schemaGenerator;
    this.datumWriterFactory = datumWriterFactory;
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    this.queueReaderFactory = queueReaderFactory;
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

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      String runIdOption = options.getArguments().getOption(ProgramOptionConstants.RUN_ID);
      Preconditions.checkNotNull(runIdOption, "Missing runId");
      RunId runId = RunIds.fromString(runIdOption);

      boolean disableTransaction = Boolean.parseBoolean(
        options.getArguments().getOption(ProgramOptionConstants.DISABLE_TRANSACTION, Boolean.toString(false)));

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

      String processorName = program.getName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      FlowSpecification flowSpec = appSpec.getFlows().get(processorName);
      FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletName);
      Preconditions.checkNotNull(flowletDef, "Definition missing for flowlet \"%s\"", flowletName);

      Class<?> clz = Class.forName(flowletDef.getFlowletSpec().getClassName(), true,
                                   program.getMainClass().getClassLoader());
      Preconditions.checkArgument(Flowlet.class.isAssignableFrom(clz), "%s is not a Flowlet.", clz);

      Class<? extends Flowlet> flowletClass = (Class<? extends Flowlet>) clz;

      // Creates tx related objects
      DataFabricFacade dataFabricFacade = disableTransaction ? dataFabricFacadeFactory.createNoTransaction(program)
                                                             : dataFabricFacadeFactory.create(program);
      DataSetContext dataSetContext = dataFabricFacade.getDataSetContext();

      // Creates flowlet context
      flowletContext = new BasicFlowletContext(program, flowletName, instanceId,
                                               runId, instanceCount,
                                               DataSets.createDataSets(dataSetContext, flowletDef.getDatasets()),
                                               options.getUserArguments(),
                                               flowletDef.getFlowletSpec(),
                                               metricsCollectionService);

      // hack for propagating metrics collector to datasets
      if (dataSetContext instanceof DataSetInstantiationBase) {
        ((DataSetInstantiationBase) dataSetContext).setMetricsCollector(flowletContext.getSystemMetrics());
      }

      // Creates QueueSpecification
      Table<Node, String, Set<QueueSpecification>> queueSpecs =
        new SimpleQueueSpecificationGenerator(Id.Application.from(program.getAccountId(), program.getApplicationId()))
          .create(flowSpec);

      Flowlet flowlet = new InstantiatorFactory(false).get(TypeToken.of(flowletClass)).create();
      TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);

      // Inject DataSet, OutputEmitter, Metric fields
      Reflections.visit(flowlet, TypeToken.of(flowlet.getClass()),
                        new PropertyFieldSetter(flowletDef.getFlowletSpec().getProperties()),
                        new DataSetFieldSetter(flowletContext),
                        new MetricsFieldSetter(flowletContext.getMetrics()),
                        new OutputEmitterFieldSetter(outputEmitterFactory(flowletContext, flowletName,
                                                                          dataFabricFacade, queueSpecs)));

      ImmutableList.Builder<QueueConsumerSupplier> queueConsumerSupplierBuilder = ImmutableList.builder();
      Collection<ProcessSpecification> processSpecs =
        createProcessSpecification(flowletContext, flowletType,
                                   processMethodFactory(flowlet),
                                   processSpecificationFactory(dataFabricFacade, queueReaderFactory, flowletName,
                                                               queueSpecs, queueConsumerSupplierBuilder,
                                                               createSchemaCache(program)),
                                   Lists.<ProcessSpecification>newLinkedList());
      List<QueueConsumerSupplier> queueConsumerSuppliers = queueConsumerSupplierBuilder.build();

      FlowletProcessDriver driver = new FlowletProcessDriver(flowlet, flowletContext, processSpecs,
                                                             createCallback(flowlet, flowletDef.getFlowletSpec()),
                                                             dataFabricFacade);

      if (disableTransaction) {
        LOG.info("Transaction disabled for flowlet {}", flowletContext);
      }
      LOG.info("Starting flowlet: {}", flowletContext);
      driver.start();
      LOG.info("Flowlet started: {}", flowletContext);


      return new FlowletProgramController(program.getName(), flowletName,
                                          flowletContext, driver, queueConsumerSuppliers);

    } catch (Exception e) {
      // something went wrong before the flowlet even started. Make sure we release all resources (datasets, ...)
      // of the flowlet context.
      if (flowletContext != null) {
        flowletContext.close();
      }
      throw Throwables.propagate(e);
    }
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
  private Collection<ProcessSpecification> createProcessSpecification(BasicFlowletContext flowletContext,
                                                                      TypeToken<? extends Flowlet> flowletType,
                                                                      ProcessMethodFactory processMethodFactory,
                                                                      ProcessSpecificationFactory processSpecFactory,
                                                                      Collection<ProcessSpecification> result)
    throws NoSuchMethodException {

    // Walk up the hierarchy of flowlet class to get all ProcessInput and Tick methods
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Extracts all process and tick methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        ProcessInput processInputAnnotation = method.getAnnotation(ProcessInput.class);
        Tick tickAnnotation = method.getAnnotation(Tick.class);

        if (processInputAnnotation == null && tickAnnotation == null) {
          // Neither a process nor a tick method.
          continue;
        }

        int maxRetries = (tickAnnotation == null) ? processInputAnnotation.maxRetries() : tickAnnotation.maxRetries();

        ProcessMethod processMethod = processMethodFactory.create(method, maxRetries);
        Set<String> inputNames;
        Schema schema;
        TypeToken<?> dataType;
        ConsumerConfig consumerConfig;
        int batchSize = 1;

        if (tickAnnotation != null) {
          inputNames = ImmutableSet.of();
          consumerConfig = new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null);
          schema = Schema.of(Schema.Type.NULL);
          dataType = TypeToken.of(void.class);
        } else {
          inputNames = Sets.newHashSet(processInputAnnotation.value());
          if (inputNames.isEmpty()) {
            // If there is no input name, it would be ANY_INPUT
            inputNames.add(FlowletDefinition.ANY_INPUT);
          }
          // If batch mode then generate schema for Iterator's parameter type
          dataType = flowletType.resolveType(method.getGenericParameterTypes()[0]);
          consumerConfig = getConsumerConfig(flowletContext, method);
          Integer processBatchSize = getBatchSize(method);

          if (processBatchSize != null) {
            Preconditions.checkArgument(dataType.getRawType().equals(Iterator.class),
                                        "Only Iterator is supported.");
            Preconditions.checkArgument(dataType.getType() instanceof ParameterizedType,
                                        "Only ParameterizedType is supported for batch Iterator.");
            dataType = flowletType.resolveType(((ParameterizedType) dataType.getType()).getActualTypeArguments()[0]);
            batchSize = processBatchSize;
          }

          try {
            schema = schemaGenerator.generate(dataType.getType());
          } catch (UnsupportedTypeException e) {
            throw Throwables.propagate(e);
          }
        }

        ProcessSpecification processSpec = processSpecFactory.create(inputNames, schema, dataType, processMethod,
                                                                     consumerConfig, batchSize, tickAnnotation);
        // Add processSpec
        if (processSpec != null) {
          result.add(processSpec);
        }
      }
    }
    Preconditions.checkArgument(!result.isEmpty(), "No process or tick method found for " + flowletType);
    return result;
  }

  /**
   * Creates a {@link ConsumerConfig} based on the method annotation and the flowlet context.
   * @param flowletContext Runtime context of the flowlet.
   * @param method The process method to inspect.
   * @return A new instance of {@link ConsumerConfig}.
   */
  private ConsumerConfig getConsumerConfig(BasicFlowletContext flowletContext, Method method) {
    // Determine input queue partition type
    HashPartition hashPartition = method.getAnnotation(HashPartition.class);
    RoundRobin roundRobin = method.getAnnotation(RoundRobin.class);
    DequeueStrategy strategy = DequeueStrategy.FIFO;
    String hashKey = null;

    Preconditions.checkArgument(!(hashPartition != null && roundRobin != null),
                                "Only one strategy allowed for process() method: %s", method.getName());

    if (hashPartition != null) {
      strategy = DequeueStrategy.HASH;
      hashKey = hashPartition.value();
      Preconditions.checkArgument(!hashKey.isEmpty(), "Partition key cannot be empty: %s", method.getName());
    } else if (roundRobin != null) {
      strategy = DequeueStrategy.ROUND_ROBIN;
    }

    return new ConsumerConfig(flowletContext.getGroupId(), flowletContext.getInstanceId(),
                              flowletContext.getInstanceCount(), strategy, hashKey);
  }

  /**
   * Returns the user specify batch size or {@code null} if not specified.
   */
  private Integer getBatchSize(Method method) {
    // Determine queue batch size, if any
    Batch batch = method.getAnnotation(Batch.class);
    if (batch != null) {
      int batchSize = batch.value();
      Preconditions.checkArgument(batchSize > 0, "Batch size should be > 0: %s", method.getName());
      return batchSize;
    }
    return null;
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

  private OutputEmitterFactory outputEmitterFactory(final BasicFlowletContext flowletContext,
                                                    final String flowletName,
                                                    final QueueClientFactory queueClientFactory,
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

              final String queueMetricsName = "process.events.out";
              final String queueMetricsTag = queueSpec.getQueueName().getSimpleName();
              Queue2Producer producer = queueClientFactory.createProducer(queueSpec.getQueueName(), new QueueMetrics() {
                @Override
                public void emitEnqueue(int count) {
                  flowletContext.getSystemMetrics().gauge(queueMetricsName, count, queueMetricsTag);
                }

                @Override
                public void emitEnqueueBytes(int bytes) {
                  // no-op
                }
              });
              return new DatumOutputEmitter<T>(producer,  schema, datumWriterFactory.create(type, schema));
            }
          }

          throw new IllegalArgumentException(String.format("No queue specification found for %s, %s",
                                                           flowletName, type));

        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private ProcessMethodFactory processMethodFactory(final Flowlet flowlet) {
    return new ProcessMethodFactory() {
      @Override
      public ProcessMethod create(Method method, int maxRetries) {
        return ReflectionProcessMethod.create(flowlet, method, maxRetries);
      }
    };
  }

  private ProcessSpecificationFactory processSpecificationFactory(
    final DataFabricFacade dataFabricFacade, final QueueReaderFactory queueReaderFactory,
    final String flowletName, final Table<Node, String, Set<QueueSpecification>> queueSpecs,
    final ImmutableList.Builder<QueueConsumerSupplier> queueConsumerSupplierBuilder,
    final SchemaCache schemaCache) {

    return new ProcessSpecificationFactory() {
      @Override
      public <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType,
                                             ProcessMethod method, ConsumerConfig consumerConfig, int batchSize,
                                             Tick tickAnnotation) {
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

              QueueConsumerSupplier consumerSupplier = new QueueConsumerSupplier(dataFabricFacade,
                                                                                 queueName, consumerConfig, numGroups);
              queueConsumerSupplierBuilder.add(consumerSupplier);
              queueReaders.add(queueReaderFactory.create(consumerSupplier, batchSize));
            }
          }
        }

        // If inputs is needed but there is no available input queue, return null
        if (!inputNames.isEmpty() && queueReaders.isEmpty()) {
          return null;
        }
        return new ProcessSpecification<T>(new RoundRobinQueueReader(queueReaders),
                                           createInputDatumDecoder(dataType, schema, schemaCache),
                                           method, tickAnnotation);
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

  private static interface ProcessMethodFactory {
    ProcessMethod create(Method method, int maxRetries);
  }

  private static interface ProcessSpecificationFactory {
    /**
     * Returns a {@link ProcessSpecification} for invoking the given process method. {@code null} is returned if
     * no input is available for the given method.
     */
    <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType,
                                    ProcessMethod method, ConsumerConfig consumerConfig, int batchSize,
                                    Tick tickAnnotation);
  }
}
