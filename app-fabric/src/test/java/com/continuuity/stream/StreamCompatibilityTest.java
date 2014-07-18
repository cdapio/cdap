/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.stream;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.common.stream.StreamEventDataCodec;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 *
 */
@Ignore
public class StreamCompatibilityTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  public static final Supplier<File> TEMP_DIR_SUPPLIER = new Supplier<File>() {

    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  private static final BlockingQueue<String> MESSAGE_QUEUE = new ArrayBlockingQueue<String>(2);

  @Test
  public void decodeOldStream() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector().createChildInjector(new StreamWriterModule());
    StreamWriter writer = injector.getInstance(StreamWriterFactory.class).create(QueueName.fromStream("stream"));
    StreamEventEncoder oldEncoder = injector.getInstance(Key.get(StreamEventEncoder.class, Names.named("old")));
    StreamEventEncoder newEncoder = injector.getInstance(Key.get(StreamEventEncoder.class, Names.named("new")));

    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(StreamApp.class,
                                                                                         TEMP_DIR_SUPPLIER);
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);

    ProgramRunner flowRunner = runnerFactory.create(ProgramRunnerFactory.Type.FLOW);
    Program flowProgram = app.getPrograms().iterator().next();
    ProgramController controller = flowRunner.run(flowProgram, new SimpleProgramOptions(flowProgram));

    // Writer to stream with old stream format
    writer.write("Old stream event", oldEncoder);
    writer.write("New stream event", newEncoder);

    Assert.assertEquals("Old stream event", MESSAGE_QUEUE.poll(10, TimeUnit.SECONDS));
    Assert.assertEquals("New stream event", MESSAGE_QUEUE.poll(10, TimeUnit.SECONDS));

    controller.stop().get();
  }

  /**
   * Encode StreamEvent to byte[] to write to Stream.
   */
  private interface StreamEventEncoder {
    byte[] encode(StreamEvent event) throws IOException;
  }

  /**
   * Encode StreamEvent as old schema.
   */
  private static final class OldStreamEventEncoder implements StreamEventEncoder {

    private final Schema schema;

    @Inject
    OldStreamEventEncoder(SchemaGenerator schemaGenerator) throws Exception {
      schema = schemaGenerator.generate(StreamEventData.class);
    }

    @Override
    public byte[] encode(StreamEvent event) throws IOException {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(os);

      // Write the schema hash
      os.write(schema.getSchemaHash().toByteArray());
      // Write the data
      StreamEventDataCodec.encode(event, encoder);

      return os.toByteArray();
    }
  }

  /**
   * Encode StreamEvent with new schema.
   */
  private static final class NewStreamEventEncoder implements StreamEventEncoder {

    private final StreamEventCodec codec = new StreamEventCodec();

    @Override
    public byte[] encode(StreamEvent event) throws IOException {
      return codec.encodePayload(event);
    }
  }

  /**
   * Factory for creating StreamWriter
   */
  private interface StreamWriterFactory {
    StreamWriter create(QueueName queueName);
  }

  private static final class StreamWriter {

    private final Queue2Producer producer;
    private final TransactionSystemClient txSystemClient;

    @Inject
    public StreamWriter(QueueClientFactory queueClientFactory,
                        TransactionSystemClient txSystemClient,
                        @Assisted QueueName queueName) throws Exception {
      this.producer = queueClientFactory.createProducer(queueName);
      this.txSystemClient = txSystemClient;
    }

    public void write(String data, StreamEventEncoder encoder) throws Exception {
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of(), Charsets.UTF_8.encode(data));
      enqueue(encoder.encode(event));
    }

    private void enqueue(byte[] bytes) throws IOException {
      TransactionAware txAware = (TransactionAware) producer;

      // start tx to write in queue in tx
      Transaction tx = txSystemClient.startShort();
      txAware.startTx(tx);
      try {
        producer.enqueue(new QueueEntry(bytes));
        if (!txSystemClient.canCommit(tx, txAware.getTxChanges())
          || !txAware.commitTx()
          || !txSystemClient.commit(tx)) {
          throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Fail to commit");
        }
        txAware.postTxCommit();
      } catch (Exception e) {
        try {
          txAware.rollbackTx();
          txSystemClient.abort(tx);
        } catch (Exception ex) {
          throw new IOException(ex);
        }
        throw new IOException(e);
      }
    }
  }

  private static final class StreamWriterModule extends AbstractModule {

    @Override
    protected void configure() {
      install(
        new FactoryModuleBuilder()
          .build(StreamWriterFactory.class)
      );
      bind(StreamEventEncoder.class).annotatedWith(Names.named("old")).to(OldStreamEventEncoder.class);
      bind(StreamEventEncoder.class).annotatedWith(Names.named("new")).to(NewStreamEventEncoder.class);
    }
  }

  /**
   *
   */
  public static final class StreamApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("StreamApp")
        .setDescription("StreamApp")
        .withStreams()
          .add(new Stream("stream"))
        .noDataSet()
        .withFlows()
          .add(new StreamFlow())
        .noProcedure()
        .noMapReduce()
        .noWorkflow()
        .build();
    }
  }

  /**
   *
   */
  public static final class StreamFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("StreamFlow")
        .setDescription("StreamFlow")
        .withFlowlets()
          .add("reader", new StreamReader())
        .connect()
          .fromStream("stream").to("reader")
        .build();
    }
  }

  /**
   *
   */
  public static final class StreamReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(StreamReader.class);

    @ProcessInput
    public void process(StreamEvent event) throws InterruptedException {
      String msg = Charsets.UTF_8.decode(event.getBody()).toString();
      LOG.info(msg);
      MESSAGE_QUEUE.put(msg);
    }
  }
}
