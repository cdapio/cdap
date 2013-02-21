/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.common.LocalLogWriter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntryImpl;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.FlowProgramRunner;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * This is a test helper for our internal test.
 * <p>
 *   <i>Note: please don't include this in the developer test</i>
 * </p>
 */
public class TestHelper {

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getCanonicalName());
    return manifest;
  }

  /**
   * @return Returns an instance of {@link LocalManager}
   */
  public static Manager<Location, ApplicationWithPrograms> getLocalManager(CConfiguration configuration) {
    LocationFactory lf = new LocalLocationFactory();
    PipelineFactory pf = new SynchronousPipelineFactory();

    final Injector injector = Guice.createInjector(new BigMamaModule(),
                                                   new DataFabricModules().getInMemoryModules());

    ManagerFactory factory = injector.getInstance(ManagerFactory.class);
    return (Manager<Location, ApplicationWithPrograms>)factory.create(configuration);
  }

  public interface Enqueuer {

  }
//
//  /**
//   * Runs an application.
//   * @param application
//   * @throws Exception
//   */
//  public static void runProgram(Class<? extends Application> application,
//                                String programName, ProgramOptions options) throws Exception {
//
//    final CConfiguration configuration = CConfiguration.create();
//    configuration.set("app.temp.dir", "/tmp/app/temp");
//    configuration.set("app.output.dir", "/tmp/app/archive" + UUID.randomUUID());
//    final Injector injector = Guice.createInjector(new BigMamaModule(),
//                                                   new DataFabricModules().getInMemoryModules(),
//                                                   new AbstractModule() {
//                                                     @Override
//                                                     protected void configure() {
//                                                       bind(LogWriter.class).toInstance(new LocalLogWriter(configuration));
//                                                     }
//                                                   }
//    );
//
//    LocationFactory lf = injector.getInstance(LocationFactory.class);
//    Location deployedJar = lf.create(
//      JarFinder.getJar(application, TestHelper.getManifestWithMainClass(application))
//    );
//
//    deployedJar.deleteOnExit();
//    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(Id.Account.DEFAULT(), deployedJar);
//    final ApplicationWithPrograms app = (ApplicationWithPrograms)p.get();
//    for (final Program program : app.getPrograms()) {
//      if(program.getProgramName().equals(programName)) {
//        ProgramRunner runner = injector.getInstance(FlowProgramRunner.class);
//        runner.run(program, options);
//        break;
//      }
//    }
//
//    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
//    OperationContext  opCtx = new OperationContext(Id.Account.DEFAULT().getId(),
//                                                  app.getAppSpecLoc().getSpecification().getName());
//
//    QueueProducer queueProducer = new QueueProducer("Testing");
//    QueueName queueName = QueueName.fromStream(Id.Account.DEFAULT(), "text");
//    StreamEventCodec codec = new StreamEventCodec();
//    for (int i = 0; i < 10; i++) {
//      String msg = "Testing message " + i;
//      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of(),
//                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
//      QueueEnqueue enqueue = new QueueEnqueue(queueProducer, queueName.toBytes(),
//                                              new QueueEntryImpl(codec.encodePayload(event)));
//      opex.commit(opCtx, enqueue);
//    }
//  }


}
