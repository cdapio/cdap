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

package io.cdap.cdap;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.preview.PreviewHttpServer;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link StandaloneMain}
 */
public class StandaloneMainTest {

  @Test
  public void testInjector() {
    try {
      System.out.println("DEBUG: Service$Listener source: " +
        com.google.common.util.concurrent.Service.Listener.class.getProtectionDomain().getCodeSource().getLocation());
      System.out.println("DEBUG: ClassReader source: " +
        org.objectweb.asm.ClassReader.class.getProtectionDomain().getCodeSource().getLocation());
    } catch (Exception e) {
      e.printStackTrace();
    }
    StandaloneMain sdk = StandaloneMain.create(CConfiguration.create(), new Configuration());
    PreviewHttpServer previewHttpServer = sdk.getInjector().getInstance(PreviewHttpServer.class);
    PreviewRunnerManager previewRunnerManager = sdk.getInjector().getInstance(PreviewRunnerManager.class);
    PreviewRunStopper previewRunStopper = sdk.getInjector().getInstance(PreviewRunStopper.class);

    Assert.assertSame(previewRunnerManager, previewRunStopper);
    TransactionManager txManager = sdk.getInjector().getInstance(TransactionManager.class);
    txManager.startAsync().awaitRunning();
    previewHttpServer.startAsync().awaitRunning();
    ((Service) previewRunnerManager).startAsync().awaitRunning();
    ((Service) previewRunnerManager).stopAsync().awaitTerminated();
    previewHttpServer.stopAsync().awaitTerminated();
    txManager.stopAsync().awaitTerminated();
  }
}
