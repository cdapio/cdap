/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import io.cdap.cdap.common.conf.CConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.FilePermission;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;

/**
 * Unit test for {@link RunnableTaskLauncher}.
 */
public class RunnableTaskLauncherTest {

  @Test
  public void testLaunchRunnableTask() throws Exception {
    String want = "test";
    RunnableTaskRequest request = new RunnableTaskRequest(TestEchoRunnableTask.class.getName(), want);

    RunnableTaskLauncher launcher = new RunnableTaskLauncher(CConfiguration.create());
    byte[] got = launcher.launchRunnableTask(request);
    Assert.assertEquals(want, new String(got, StandardCharsets.UTF_8));
  }

  @Test
  public void testRunnableTaskWithValidPermission() throws Exception {
    String want = "/tmp/test.txt";
    RunnableTaskRequest request = new RunnableTaskRequest(TestFilePermissionRunnableTask.class.getName(), want);

    RunnableTaskLauncher launcher = new RunnableTaskLauncher(CConfiguration.create());
    byte[] got = launcher.launchRunnableTask(request);
    Assert.assertEquals(want, new String(got, StandardCharsets.UTF_8));

    want = System.getProperty("user.home") + "/test.txt";
    request = new RunnableTaskRequest(TestFilePermissionRunnableTask.class.getName(), want);

    launcher = new RunnableTaskLauncher(CConfiguration.create());
    got = launcher.launchRunnableTask(request);
    Assert.assertEquals(want, new String(got, StandardCharsets.UTF_8));
  }

  @Test(expected = AccessControlException.class)
  public void testRunnableTaskWithInvalidPermission() throws Exception {
    String want = "/usr/test.txt";
    RunnableTaskRequest request = new RunnableTaskRequest(TestFilePermissionRunnableTask.class.getName(), want);

    RunnableTaskLauncher launcher = new RunnableTaskLauncher(CConfiguration.create());
    byte[] got = launcher.launchRunnableTask(request);
    Assert.fail();
  }

  public static class TestEchoRunnableTask extends RunnableTask {
    @Override
    protected byte[] run(String param) throws Exception {
      return param.getBytes();
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }
  }

  public static class TestFilePermissionRunnableTask extends RunnableTask {
    @Override
    protected byte[] run(String param) throws Exception {
      FilePermission fp = new FilePermission(param, "read");
      System.getSecurityManager().checkPermission(fp);
      return param.getBytes();
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }
  }

}
