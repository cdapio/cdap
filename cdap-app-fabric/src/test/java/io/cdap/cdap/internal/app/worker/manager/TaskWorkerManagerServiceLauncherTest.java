/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.manager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.master.spi.twill.ExtendedTwillPreparer;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

/**
 * Unit test for {@link TaskWorkerManagerServiceLauncher}.
 */
public class TaskWorkerManagerServiceLauncherTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private CConfiguration cConf;
  private TwillRunner twillRunner;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, temporaryFolder.newFolder().getAbsolutePath());

    twillRunner = mock(TwillRunner.class);
    when(twillRunner.lookup(TaskWorkerManagerTwillApplication.NAME))
        .thenReturn(Collections.emptyList());
  }

  /**
   * The proxy keeps its namespace-to-pod lease table in memory, so two live pods hand the same
   * task worker to two namespaces. A replica count of one does not prevent that during a rollout,
   * because the default RollingUpdate strategy surges to a second pod first. This test guards the
   * only thing that does prevent it.
   */
  @Test
  public void testLaunchRequestsRecreateStrategy() {
    ExtendedTwillPreparer preparer = mockPreparer(ExtendedTwillPreparer.class);
    TwillController controller = mock(TwillController.class);
    when(preparer.start(anyLong(), any(TimeUnit.class))).thenReturn(controller);
    when(twillRunner.prepare(any(TwillApplication.class))).thenReturn(preparer);

    new TaskWorkerManagerServiceLauncher(cConf, new Configuration(), twillRunner).run();

    // Ordering matters: the strategy has to be set while the preparer is still being configured,
    // not after the deployment has already been submitted.
    InOrder inOrder = Mockito.inOrder(preparer);
    inOrder.verify(preparer).withRecreateStrategy();
    inOrder.verify(preparer).start(anyLong(), any(TimeUnit.class));
  }

  /**
   * Only the Kubernetes preparer implements {@link ExtendedTwillPreparer}. On any other Twill
   * runtime the launch must still go through rather than failing on a bad cast.
   */
  @Test
  public void testLaunchWithoutExtendedPreparerStillStarts() {
    TwillPreparer preparer = mockPreparer(TwillPreparer.class);
    TwillController controller = mock(TwillController.class);
    when(preparer.start(anyLong(), any(TimeUnit.class))).thenReturn(controller);
    when(twillRunner.prepare(any(TwillApplication.class))).thenReturn(preparer);

    new TaskWorkerManagerServiceLauncher(cConf, new Configuration(), twillRunner).run();

    verify(preparer).start(anyLong(), any(TimeUnit.class));
  }

  /**
   * The launcher runs on a fixed-rate schedule, so every iteration after the first must be a
   * no-op. Launching a second application would defeat the single-instance guarantee just as
   * surely as a surging rollout would.
   */
  @Test
  public void testExistingControllerIsNotRelaunched() {
    TwillController existing = mock(TwillController.class);
    when(twillRunner.lookup(TaskWorkerManagerTwillApplication.NAME))
        .thenReturn(Collections.singletonList(existing));

    new TaskWorkerManagerServiceLauncher(cConf, new Configuration(), twillRunner).run();

    verify(twillRunner, never()).prepare(any(TwillApplication.class));
    verify(existing, never()).terminate();
  }

  /**
   * If a previous App Fabric generation left a proxy behind, the extras are terminated so that
   * exactly one remains.
   */
  @Test
  public void testDuplicateControllersAreTerminated() {
    TwillController first = mock(TwillController.class);
    TwillController second = mock(TwillController.class);
    when(twillRunner.lookup(TaskWorkerManagerTwillApplication.NAME))
        .thenReturn(Arrays.asList(first, second));

    new TaskWorkerManagerServiceLauncher(cConf, new Configuration(), twillRunner).run();

    // The first controller found is adopted; every other one is surplus.
    verify(first, never()).terminate();
    verify(second).terminate();
    verify(twillRunner, never()).prepare(any(TwillApplication.class));
  }

  /**
   * The proxy pod runs its own KubeMasterEnvironment and needs to discover task worker pods
   * directly via endpoints rather than the service ClusterIP. In addition, internal SSL cert path
   * must be stripped as it is not mounted into the proxy pod.
   */
  @Test
  public void testLaunchConfiguresCConfForProxy() throws Exception {
    cConf.set(Constants.Security.SSL.INTERNAL_CERT_PATH, "/path/to/cert");
    TwillPreparer preparer = mockPreparer(TwillPreparer.class);
    TwillController controller = mock(TwillController.class);
    when(preparer.start(anyLong(), any(TimeUnit.class))).thenReturn(controller);
    ArgumentCaptor<TwillApplication> appCaptor = ArgumentCaptor.forClass(TwillApplication.class);
    when(twillRunner.prepare(appCaptor.capture())).thenReturn(preparer);

    new TaskWorkerManagerServiceLauncher(cConf, new Configuration(), twillRunner).run();

    TaskWorkerManagerTwillApplication app =
        (TaskWorkerManagerTwillApplication) appCaptor.getValue();
    CConfiguration launchedCConf = CConfiguration.create(new File(app.getCConfFileUri()));

    Assert.assertEquals(Constants.Service.TASK_WORKER,
        launchedCConf.get(Constants.TaskWorkerManager.ENDPOINTS_SERVICES));
    Assert.assertNull(launchedCConf.get(Constants.Security.SSL.INTERNAL_CERT_PATH));
  }

  /**
   * Returns a preparer mock whose fluent setters return the mock itself, matching the contract
   * every {@link TwillPreparer} implementation follows.
   */
  private <T extends TwillPreparer> T mockPreparer(Class<T> preparerClass) {
    return mock(preparerClass, withSettings().defaultAnswer(Answers.RETURNS_SELF));
  }
}
