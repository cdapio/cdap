/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.spark.distributed.DistributedSparkProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.filesystem.Location;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class DistributedProgramRunnerTxTimeoutTest {

  private static ApplicationSpecification appSpec;
  private static CConfiguration cConf = CConfiguration.create();
  private static YarnConfiguration yConf = new YarnConfiguration();

  private static Program flow = createProgram(ProgramType.FLOW, "flow");
  private static Program service = createProgram(ProgramType.SERVICE, "service");
  private static Program worker = createProgram(ProgramType.WORKER, "worker");
  private static Program mapreduce = createProgram(ProgramType.MAPREDUCE, "mapreduce");
  private static Program spark = createProgram(ProgramType.SPARK, "spark");
  private static Program workflow = createProgram(ProgramType.WORKER, "workflow");

  private static DistributedProgramRunner flowRunner;
  private static DistributedProgramRunner serviceRunner;
  private static DistributedProgramRunner workerRunner;
  private static DistributedProgramRunner mapreduceRunner;
  private static DistributedProgramRunner sparkRunner;
  private static DistributedProgramRunner workflowRunner;

  @BeforeClass
  public static void setup() {
    Application app = new AppWithAllProgramTypes();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(
      Id.Namespace.DEFAULT, new Id.Artifact(Id.Namespace.DEFAULT, "artifact", new ArtifactVersion("0.1")), app);
    app.configure(configurer, new ApplicationContext() {
      @Override
      public Config getConfig() {
        return null;
      }
    });
    appSpec = configurer.createSpecification("app", "1.0");
    // System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(appSpec));

    cConf.setInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, 60);
    flowRunner = new DistributedFlowProgramRunner(null, yConf, cConf, null, null, null, null, null);
    serviceRunner = new DistributedServiceProgramRunner(null, yConf, cConf, null, null);
    workerRunner = new DistributedWorkerProgramRunner(null, yConf, cConf, null, null);
    mapreduceRunner = new DistributedMapReduceProgramRunner(null, yConf, cConf, null, null);
    sparkRunner = new DistributedSparkProgramRunner(null, yConf, cConf, null, null);
    workflowRunner = new DistributedWorkflowProgramRunner(null, yConf, cConf, null, null, null);
  }

  @Test
  public void testValid() {
    flowRunner.validateOptions(flow, createOptions(flow));
    flowRunner.validateOptions(flow, createOptions(flow, 30));
    flowRunner.validateOptions(flow, createOptions(flow, 1000, "flowlet", "nosuch"));

    serviceRunner.validateOptions(service, createOptions(service));
    serviceRunner.validateOptions(service, createOptions(service, 30));

    workerRunner.validateOptions(worker, createOptions(worker));
    workerRunner.validateOptions(worker, createOptions(worker, 30));

    mapreduceRunner.validateOptions(mapreduce, createOptions(mapreduce));
    mapreduceRunner.validateOptions(mapreduce, createOptions(mapreduce, 30));

    sparkRunner.validateOptions(spark, createOptions(spark));
    sparkRunner.validateOptions(spark, createOptions(spark, 30));

    workflowRunner.validateOptions(workflow, createOptions(workflow));
    workflowRunner.validateOptions(workflow, createOptions(workflow, 30));
    workflowRunner.validateOptions(workflow, createOptions(workflow, 30, "action", "nosuch"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFlow() {
    flowRunner.validateOptions(flow, createOptions(flow, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFlowlet() {
    flowRunner.validateOptions(flow, createOptions(flow, 61, "flowlet", "ticker"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidService() {
    serviceRunner.validateOptions(service, createOptions(service, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMapreduce() {
    mapreduceRunner.validateOptions(mapreduce, createOptions(mapreduce, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSpark() {
    sparkRunner.validateOptions(spark, createOptions(spark, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWorker() {
    workerRunner.validateOptions(worker, createOptions(worker, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWorkflow() {
    workflowRunner.validateOptions(workflow, createOptions(workflow, 61));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAction() {
    workflowRunner.validateOptions(workflow, createOptions(workflow, 61, "action", "noop"));
  }

  private static ProgramOptions createOptions(Program program) {
    return new SimpleProgramOptions(program.getId());
  }

  private static ProgramOptions createOptions(Program program, int timeout) {
    return new SimpleProgramOptions(program.getId().getProgram(),
                                    new BasicArguments(Collections.<String, String>emptyMap()),
                                    new BasicArguments(Collections.singletonMap(SystemArguments.TRANSACTION_TIMEOUT,
                                                                                String.valueOf(timeout))));
  }

  private static ProgramOptions createOptions(Program program, int timeout, String scope, String name) {
    return new SimpleProgramOptions(program.getId().getProgram(),
                                    new BasicArguments(Collections.<String, String>emptyMap()),
                                    new BasicArguments(Collections.singletonMap(
                                      RuntimeArguments.addScope(scope, name, SystemArguments.TRANSACTION_TIMEOUT),
                                      String.valueOf(timeout))));
  }

  private static Program createProgram(final ProgramType type, final String name) {
    return new Program() {
      @Override
      public String getMainClassName() {
        return null;
      }
      @Override
      public <T> Class<T> getMainClass() throws ClassNotFoundException {
        return null;
      }
      @Override
      public ProgramType getType() {
        return type;
      }
      @Override
      public ProgramId getId() {
        return new ProgramId(getNamespaceId(), getApplicationId(), getType(), getName());
      }
      @Override
      public String getName() {
        return name;
      }
      @Override
      public String getNamespaceId() {
        return Id.Namespace.DEFAULT.getId();
      }
      @Override
      public String getApplicationId() {
        return appSpec.getName();
      }
      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return appSpec;
      }
      @Override
      public Location getJarLocation() {
        return null;
      }
      @Override
      public ClassLoader getClassLoader() {
        return null;
      }
      @Override
      public void close() throws IOException {
      }
    };
  }

  public static class AppWithAllProgramTypes extends AbstractApplication {
    @Override
    public void configure() {
      setName("app");
      addFlow(new AbstractFlow() {
        @Override
        protected void configure() {
          setName("flow");
          addFlowlet(new Ticker());
          addFlowlet(new Counter());
          connect("ticker", "counter");
        }
      });
      addWorker(new AbstractWorker() {
        @Override
        public void configure() {
          setName("worker");
        }
        @Override
        public void run() {
          // no-op
        }
      });
      addService(new AbstractService() {
        @Override
        protected void configure() {
          setName("service");
          addHandler(new AbstractHttpServiceHandler() {
            @Override
            public void configure() {
              setName("handler");
            }
          });
        }
      });
      addMapReduce(new AbstractMapReduce() {
        @Override
        protected void configure() {
          setName("mapreduce");
        }
      });
      addSpark(new AbstractSpark() {
        @Override
        protected void configure() {
          setName("spark");
          setMainClassName("mainclass");
        }
      });
      addWorkflow(new AbstractWorkflow() {
        @Override
        protected void configure() {
          setName("workflow");
          addAction(new AbstractCustomAction("noop") {
            @Override
            public void run() throws Exception {
              // no-op
            }
          });
        }
      });
    }
  }

  public static class Ticker extends AbstractFlowlet {
    private OutputEmitter<Integer> out;
    @Override
    protected void configure() {
      setName("ticker");
    }
    @Tick(delay = 100)
    public void tick() {
      out.emit(1);
    }
  }

  public static class Counter extends AbstractFlowlet {
    @Override
    protected void configure() {
      setName("counter");
    }
    @ProcessInput
    public void process(int i) {
      // no-op
    }
  }
}

