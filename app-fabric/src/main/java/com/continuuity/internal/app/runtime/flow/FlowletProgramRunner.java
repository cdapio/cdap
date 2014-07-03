/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

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
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator.Node;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.async.ExecutorUtils;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamPropertyListener;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.RoundRobinQueueReader;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.DataSetFieldSetter;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.internal.specification.FlowletMethod;
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
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 *
 */
public final class FlowletProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProgramRunner.class);

  private final SchemaGenerator schemaGenerator;
  private final DatumWriterFactory datumWriterFactory;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;
  private final StreamCoordinator streamCoordinator;
  private final QueueReaderFactory queueReaderFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramServiceDiscovery serviceDiscovery;

  @Inject
  public FlowletProgramRunner(SchemaGenerator schemaGenerator,
                              DatumWriterFactory datumWriterFactory,
                              DataFabricFacadeFactory dataFabricFacadeFactory, StreamCoordinator streamCoordinator,
                              QueueReaderFactory queueReaderFactory,
                              MetricsCollectionService metricsCollectionService,
                              ProgramServiceDiscovery serviceDiscovery) {
    this.schemaGenerator = schemaGenerator;
    this.datumWriterFactory = datumWriterFactory;
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    this.streamCoordinator = streamCoordinator;
    this.queueReaderFactory = queueReaderFactory;
    this.metricsCollectionService = metricsCollectionService;
    this.serviceDiscovery = serviceDiscovery;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  void setLogWriter(LogWriter logWriter) {
    CAppender.logWriter = logWriter;
  }

  @SuppressWarnings("unchecked")
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
                                               options.getUserArguments(), flowletDef.getFlowletSpec(),
                                               metricsCollectionService, serviceDiscovery);

      // hack for propagating metrics collector to datasets
      if (dataSetContext instanceof DataSetInstantiationBase) {
        ((DataSetInstantiationBase) dataSetContext).setMetricsCollector(metricsCollectionService,
                                                                        flowletContext.getSystemMetrics());
      }

      // Creates QueueSpecification
      Table<Node, String, Set<QueueSpecification>> queueSpecs =
        new SimpleQueueSpecificationGenerator(Id.Application.from(program.getAccountId(), program.getApplicationId()))
          .create(flowSpec);

      Flowlet flowlet = new InstantiatorFactory(false).get(TypeToken.of(flowletClass)).create();
      TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);

      // Set the context classloader to the reactor classloader. It is needed for the DatumWriterFactory be able
      // to load reactor classes
      Thread.currentThread().setContextClassLoader(FlowletProgramRunner.class.getClassLoader());

      // Inject DataSet, OutputEmitter, Metric fields
      Reflections.visit(flowlet, TypeToken.of(flowlet.getClass()),
                        new PropertyFieldSetter(flowletDef.getFlowletSpec().getProperties()),
                        new DataSetFieldSetter(flowletContext),
                        new MetricsFieldSetter(flowletContext.getMetrics()),
                        new OutputEmitterFieldSetter(outputEmitterFactory(flowletContext, flowletName,
                                                                          dataFabricFacade, queueSpecs)));

      ImmutableList.Builder<ConsumerSupplier<?>> queueConsumerSupplierBuilder = ImmutableList.builder();
      Collection<ProcessSpecification> processSpecs =
        createProcessSpecification(flowletContext, flowletType,
                                   processMethodFactory(flowlet),
                                   processSpecificationFactory(flowletContext, dataFabricFacade, queueReaderFactory,
                                                               flowletName, queueSpecs, queueConsumerSupplierBuilder,
                                                               createSchemaCache(program)),
                                   Lists.<ProcessSpecification>newLinkedList());
      List<ConsumerSupplier<?>> consumerSuppliers = queueConsumerSupplierBuilder.build();

      // Create the flowlet driver
      AtomicReference<FlowletProgramController> controllerRef = new AtomicReference<FlowletProgramController>();
      Service serviceHook = createServiceHook(flowletName, consumerSuppliers, controllerRef);
      FlowletProcessDriver driver = new FlowletProcessDriver(flowlet, flowletContext, processSpecs,
                                                             createCallback(flowlet, flowletDef.getFlowletSpec()),
                                                             dataFabricFacade, serviceHook);

      if (disableTransaction) {
        LOG.info("Transaction disabled for flowlet {}", flowletContext);
      }

      FlowletProgramController controller = new FlowletProgramController(program.getName(), flowletName,
                                                                         flowletContext, driver, consumerSuppliers);
      controllerRef.set(controller);

      LOG.info("Starting flowlet: {}", flowletContext);
      driver.start();
      LOG.info("Flowlet started: {}", flowletContext);

      return controller;

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
  @SuppressWarnings("unchecked")
  private Collection<ProcessSpecification> createProcessSpecification(BasicFlowletContext flowletContext,
                                                                      TypeToken<? extends Flowlet> flowletType,
                                                                      ProcessMethodFactory processMethodFactory,
                                                                      ProcessSpecificationFactory processSpecFactory,
                                                                      Collection<ProcessSpecification> result)
    throws NoSuchMethodException {

    Set<FlowletMethod> seenMethods = Sets.newHashSet();

    // Walk up the hierarchy of flowlet class to get all ProcessInput and Tick methods
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Extracts all process and tick methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        if (!seenMethods.add(new FlowletMethod(method, flowletType))) {
          // The method is already seen. It can only happen if a children class override a parent class method and
          // is visting the parent method, since the method visiting order is always from the leaf class walking
          // up the class hierarchy.
          continue;
        }

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
            if (dataType.getRawType().equals(Iterator.class)) {
              Preconditions.checkArgument(dataType.getType() instanceof ParameterizedType,
                                          "Only ParameterizedType is supported for batch Iterator.");
              dataType = flowletType.resolveType(((ParameterizedType) dataType.getType()).getActualTypeArguments()[0]);
            }
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
              return new DatumOutputEmitter<T>(producer, schema, datumWriterFactory.create(type, schema));
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
      public <T> ProcessMethod<T> create(Method method, int maxRetries) {
        return ReflectionProcessMethod.create(flowlet, method, maxRetries);
      }
    };
  }

  private ProcessSpecificationFactory processSpecificationFactory(
    final BasicFlowletContext flowletContext, final DataFabricFacade dataFabricFacade,
    final QueueReaderFactory queueReaderFactory, final String flowletName,
    final Table<Node, String, Set<QueueSpecification>> queueSpecs,
    final ImmutableList.Builder<ConsumerSupplier<?>> queueConsumerSupplierBuilder,
    final SchemaCache schemaCache) {

    return new ProcessSpecificationFactory() {
      @Override
      public <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType,
                                             ProcessMethod<T> method, ConsumerConfig consumerConfig, int batchSize,
                                             Tick tickAnnotation) {
        List<QueueReader<T>> queueReaders = Lists.newLinkedList();

        for (Map.Entry<Node, Set<QueueSpecification>> entry : queueSpecs.column(flowletName).entrySet()) {
          for (QueueSpecification queueSpec : entry.getValue()) {
            final QueueName queueName = queueSpec.getQueueName();

            if (queueSpec.getInputSchema().equals(schema)
              && (inputNames.contains(queueName.getSimpleName())
              || inputNames.contains(FlowletDefinition.ANY_INPUT))) {

              if (entry.getKey().getType() == FlowletConnection.Type.STREAM) {
                ConsumerSupplier<StreamConsumer> consumerSupplier = ConsumerSupplier.create(dataFabricFacade,
                                                                                            queueName, consumerConfig);
                queueConsumerSupplierBuilder.add(consumerSupplier);
                // No decoding is needed, as a process method can only have StreamEvent as type for consuming stream
                Function<StreamEvent, T> decoder = wrapInputDecoder(flowletContext,
                                                                    queueName, new Function<StreamEvent, T>() {
                  @Override
                  @SuppressWarnings("unchecked")
                  public T apply(StreamEvent input) {
                    return (T) input;
                  }
                });

                queueReaders.add(queueReaderFactory.createStreamReader(consumerSupplier, batchSize, decoder));

              } else {
                int numGroups = getNumGroups(Iterables.concat(queueSpecs.row(entry.getKey()).values()), queueName);
                Function<ByteBuffer, T> decoder =
                  wrapInputDecoder(flowletContext, queueName, createInputDatumDecoder(dataType, schema, schemaCache));

                ConsumerSupplier<Queue2Consumer> consumerSupplier = ConsumerSupplier.create(dataFabricFacade, queueName,
                                                                                            consumerConfig, numGroups);
                queueConsumerSupplierBuilder.add(consumerSupplier);
                queueReaders.add(queueReaderFactory.createQueueReader(consumerSupplier, batchSize, decoder));
              }
            }
          }
        }

        // If inputs is needed but there is no available input queue, return null
        if (!inputNames.isEmpty() && queueReaders.isEmpty()) {
          return null;
        }
        return new ProcessSpecification<T>(new RoundRobinQueueReader<T>(queueReaders), method, tickAnnotation);
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

  private <S, T> Function<S, T> wrapInputDecoder(final BasicFlowletContext context,
                                                 final QueueName queueName,
                                                 final Function<S, T> inputDecoder) {
    final String eventsMetricsName = "process.events.in";
    final String eventsMetricsTag = queueName.getSimpleName();
    return new Function<S, T>() {
      @Override
      public T apply(S source) {
        context.getSystemMetrics().gauge(eventsMetricsName, 1, eventsMetricsTag);
        context.getSystemMetrics().gauge("process.tuples.read", 1, eventsMetricsTag);
        return inputDecoder.apply(source);
      }
    };
  }


  private SchemaCache createSchemaCache(Program program) throws Exception {
    ImmutableSet.Builder<Schema> schemas = ImmutableSet.builder();

    for (FlowSpecification flowSpec : program.getSpecification().getFlows().values()) {
      for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
        schemas.addAll(Iterables.concat(flowletDef.getInputs().values()));
        schemas.addAll(Iterables.concat(flowletDef.getOutputs().values()));
      }
    }

    // Temp fix for ENG-3949. Always add old stream event schema.
    // TODO: Remove it later. The right thing to do is to have schemas history being stored to support schema
    // evolution. By design, as long as the schema cache is populated with old schema, the type projection logic
    // in the decoder would handle it correctly.
    schemas.add(schemaGenerator.generate(StreamEventData.class));


    return new SchemaCache(schemas.build(), program.getMainClass().getClassLoader());
  }

  /**
   * Create a initializer to be executed during the flowlet driver initialization.
   */
  private Service createServiceHook(String flowletName, Iterable<ConsumerSupplier<?>> consumerSuppliers,
                                    AtomicReference<FlowletProgramController> controller) {
    final List<String> streams = Lists.newArrayList();
    for (ConsumerSupplier<?> consumerSupplier : consumerSuppliers) {
      QueueName queueName = consumerSupplier.getQueueName();
      if (queueName.isStream()) {
        streams.add(queueName.getSimpleName());
      }
    }

    // If no stream, returns a no-op Service
    if (streams.isEmpty()) {
      return new AbstractService() {
        @Override
        protected void doStart() {
          notifyStarted();
        }

        @Override
        protected void doStop() {
          notifyStopped();
        }
      };
    }
    return new FlowletServiceHook(flowletName, streamCoordinator, streams, controller);
  }

  private static interface ProcessMethodFactory {
    <T> ProcessMethod<T> create(Method method, int maxRetries);
  }

  private static interface ProcessSpecificationFactory {
    /**
     * Returns a {@link ProcessSpecification} for invoking the given process method. {@code null} is returned if
     * no input is available for the given method.
     */
    <T> ProcessSpecification create(Set<String> inputNames, Schema schema, TypeToken<T> dataType,
                                    ProcessMethod<T> method, ConsumerConfig consumerConfig, int batchSize,
                                    Tick tickAnnotation);
  }

  /**
   * This service is for start/stop listening to changes in stream property, through the help of
   * {@link StreamCoordinator}, so that it can react to changes and properly reconfigure stream consumers used by
   * the flowlet. This hook is provided to {@link FlowletProcessDriver} and being start/stop
   * when the driver start/stop.
   */
  private static final class FlowletServiceHook extends AbstractService {

    private final StreamCoordinator streamCoordinator;
    private final List<String> streams;
    private final AtomicReference<FlowletProgramController> controller;
    private final Executor executor;
    private final Lock suspendLock = new ReentrantLock();
    private final StreamPropertyListener propertyListener;
    private Cancellable cancellable;

    private FlowletServiceHook(final String flowletName, StreamCoordinator streamCoordinator, List<String> streams,
                               AtomicReference<FlowletProgramController> controller) {
      this.streamCoordinator = streamCoordinator;
      this.streams = streams;
      this.controller = controller;
      this.executor = ExecutorUtils.newThreadExecutor(Threads.createDaemonThreadFactory("flowlet-stream-update-%d"));
      this.propertyListener = new StreamPropertyListener() {
        @Override
        public void ttlChanged(String streamName, long ttl) {
          LOG.debug("TTL for stream '{}' changed to {} for flowlet '{}'", streamName, ttl, flowletName);
          suspendAndResume();
        }

        @Override
        public void ttlDeleted(String streamName) {
          LOG.debug("TTL for stream '{}' deleted for flowlet '{}'", streamName, flowletName);
          suspendAndResume();
        }

        @Override
        public void generationChanged(String streamName, int generation) {
          LOG.debug("Generation for stream '{}' changed to {} for flowlet '{}'", streamName, generation, flowletName);
          suspendAndResume();
        }

        @Override
        public void generationDeleted(String streamName) {
          LOG.debug("Generation for stream '{}' deleted for flowlet '{}'", streamName, flowletName);
          suspendAndResume();
        }
      };
    }

    @Override
    protected void doStart() {
      final List<Cancellable> cancellables = Lists.newArrayList();
      this.cancellable = new Cancellable() {
        @Override
        public void cancel() {
          for (Cancellable c : cancellables) {
            c.cancel();
          }
        }
      };

      for (String stream : streams) {
        cancellables.add(streamCoordinator.addListener(stream, propertyListener));
      }
      notifyStarted();
    }

    @Override
    protected void doStop() {
      if (cancellable != null) {
        cancellable.cancel();
      }
      notifyStopped();
    }

    private void suspendAndResume() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          suspendLock.lock();
          try {
            controller.get().suspend().get();
            controller.get().resume().get();
          } catch (Exception e) {
            LOG.error("Failed to suspend and resume flowlet.", e);
          } finally {
            suspendLock.unlock();
          }
        }
      });
    }
  }
}
