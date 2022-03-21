/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SystemWorkerTwillRunnableTest {
  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    Injector injector = SystemWorkerTwillRunnable.createInjector(CConfiguration.create(), new Configuration());
    injector.getInstance(ArtifactManagerFactory.class).create(NamespaceId.SYSTEM, RetryStrategies.noRetry());
  }
}
