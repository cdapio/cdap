/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;

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
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getName());
    return manifest;
  }

  /**
   * @return Returns an instance of {@link LocalManager}
   */
  public static Manager<Location, ApplicationWithPrograms> getLocalManager(CConfiguration configuration) {
    LocationFactory lf = new LocalLocationFactory();
    PipelineFactory pf = new SynchronousPipelineFactory();

    final Injector injector = Guice.createInjector(new BigMamaModule(configuration),
                                                   new DataFabricModules().getInMemoryModules());


    ManagerFactory factory = injector.getInstance(ManagerFactory.class);
    return (Manager<Location, ApplicationWithPrograms>)factory.create();
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
