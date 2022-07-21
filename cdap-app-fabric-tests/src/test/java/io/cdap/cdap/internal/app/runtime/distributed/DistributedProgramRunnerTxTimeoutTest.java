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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.spark.distributed.DistributedSparkProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.runtime.spi.SparkCompat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TxConstants;
import org.apache.twill.filesystem.Location;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class DistributedProgramRunnerTxTimeoutTest {

  private static ApplicationSpecification appSpec;
  private static CConfiguration cConf = CConfiguration.create();
  private static YarnConfiguration yConf = new YarnConfiguration();

  private static Program service = createProgram(ProgramType.SERVICE, "service");
  private static Program worker = createProgram(ProgramType.WORKER, "worker");
  private static Program mapreduce = createProgram(ProgramType.MAPREDUCE, "mapreduce");
  private static Program spark = createProgram(ProgramType.SPARK, "spark");
  private static Program workflow = createProgram(ProgramType.WORKFLOW, "workflow");

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
    app.configure(configurer, () -> null);
    appSpec = configurer.createSpecification("app", "1.0");
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));

    cConf.setInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, 60);
    serviceRunner = new DistributedServiceProgramRunner(cConf, yConf, null, ClusterMode.ON_PREMISE, null,
                                                        injector);
    workerRunner = new DistributedWorkerProgramRunner(cConf, yConf, null, ClusterMode.ON_PREMISE, null,
                                                      injector);
    mapreduceRunner = new DistributedMapReduceProgramRunner(cConf, yConf, null, ClusterMode.ON_PREMISE, null,
                                                            injector);
    sparkRunner = new DistributedSparkProgramRunner(SparkCompat.SPARK2_2_11, cConf, yConf, null, null,
                                                    ClusterMode.ON_PREMISE, null, injector);
    workflowRunner = new DistributedWorkflowProgramRunner(cConf, yConf, null, ClusterMode.ON_PREMISE, null, null,
                                                          injector);
  }

  @Test
  public void testValid() {
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
    return new SimpleProgramOptions(program.getId(),
                                    new BasicArguments(Collections.emptyMap()),
                                    new BasicArguments(Collections.singletonMap(SystemArguments.TRANSACTION_TIMEOUT,
                                                                                String.valueOf(timeout))));
  }

  private static ProgramOptions createOptions(Program program, int timeout, String scope, String name) {
    return new SimpleProgramOptions(program.getId(),
                                    new BasicArguments(Collections.emptyMap()),
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
      public <T> Class<T> getMainClass() {
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
      public void close() {
      }
    };
  }

  public static class AppWithAllProgramTypes extends AbstractApplication {
    @Override
    public void configure() {
      setName("app");
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
          addHandler(new AbstractHttpServiceHandler() { });
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
            public void run() {
              // no-op
            }
          });
        }
      });
    }
  }
}

