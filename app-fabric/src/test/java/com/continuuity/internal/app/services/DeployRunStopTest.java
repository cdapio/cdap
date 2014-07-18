package com.continuuity.internal.app.services;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@Ignore
public class DeployRunStopTest {

  private static AppFabricHttpHandler server;
  private static LocationFactory lf;
  private static StoreFactory sFactory;
  private static AtomicInteger instanceCount = new AtomicInteger(0);
  private static AtomicInteger messageCount = new AtomicInteger(0);
  private static Semaphore messageSemaphore = new Semaphore(0);

  /**
   *
   */
  public static final class GenSinkApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("GenSinkApp")
        .setDescription("GenSinkApp desc")
        .noStream()
        .noDataSet()
        .withFlows().add(new GenSinkFlow())
        .noProcedure()
        .noMapReduce()
        .noWorkflow()
        .build();
    }


    /**
     *
     */
    public static final class GenSinkFlow implements Flow {

      @Override
      public FlowSpecification configure() {
        return FlowSpecification.Builder.with()
          .setName("GenSinkFlow")
          .setDescription("GenSinkFlow desc")
          .withFlowlets()
          .add(new GenFlowlet())
          .add(new SinkFlowlet())
          .connect()
          .from(new GenFlowlet()).to(new SinkFlowlet())
          .build();
      }
    }

    /**
     *
     */
    public static final class GenFlowlet extends AbstractFlowlet {

      private OutputEmitter<String> output;
      private int i;

      @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
      public void generate() throws Exception {
        if (i < 10000) {
          output.emit("Testing " + ++i);
          if (i == 10000) {
            throw new IllegalStateException("10000 hitted");
          }
        }
      }
    }

    /**
     *
     */
    public static final class SinkFlowlet extends AbstractFlowlet {

      private static final Logger LOG = LoggerFactory.getLogger(SinkFlowlet.class);

      @Override
      public void initialize(FlowletContext context) throws FlowletException {
        instanceCount.incrementAndGet();
      }

      @ProcessInput
      public void process(String text) {
        if (messageCount.incrementAndGet() == 5000) {
          messageSemaphore.release();
        } else if (messageCount.get() == 9999) {
          messageSemaphore.release();
        }
      }
    }
  }

  @Test
  public void testDeployRunStop() throws Exception {
    AppFabricTestHelper.deployApplication(GenSinkApp.class);
    AppFabricTestHelper.startProgram(server, "GenSinkApp", "GenSinkFlow", "flows", ImmutableMap.<String, String>of());
    messageSemaphore.tryAcquire(5, TimeUnit.SECONDS);
    AppFabricTestHelper.setFlowletInstances(server, "GenSinkApp", "GenSinkFlow", "SinkFlowlet", (short) 3);
    messageSemaphore.tryAcquire(5, TimeUnit.SECONDS);

    // TODO: The flow test need to reform later using the new test framework.
    // Sleep one extra seconds to consume any unconsumed items (there shouldn't be, but this is for catching error).
    TimeUnit.SECONDS.sleep(1);
    AppFabricTestHelper.stopProgram(server, "GenSinkApp", "GenSinkFlow", "flows");
    Assert.assertEquals(9999, messageCount.get());
    Assert.assertEquals(3, instanceCount.get());
  }

  @BeforeClass
  public static void before() throws Exception {
    final Injector injector = AppFabricTestHelper.getInjector();

    server = injector.getInstance(AppFabricHttpHandler.class);

    // Create location factory.
    lf = injector.getInstance(LocationFactory.class);

    // Create store
    sFactory = injector.getInstance(StoreFactory.class);
  }
}
