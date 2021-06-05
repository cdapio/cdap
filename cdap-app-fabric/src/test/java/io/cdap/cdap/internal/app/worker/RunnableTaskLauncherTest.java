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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Unit test for {@link RunnableTaskLauncher}.
 */
public class RunnableTaskLauncherTest {

  @Test
  public void testLaunchRunnableTask() throws Exception {
    String want = "test-want";
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(TestRunnableTask.class.getName()).
      withParam(want).build();

    RunnableTaskLauncher launcher = new RunnableTaskLauncher(CConfiguration.create());
    byte[] got = launcher.launchRunnableTask(request, null);
    Assert.assertEquals(want, new String(got, StandardCharsets.UTF_8));
  }

  public static class TestRunnableTask implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      context.writeResult(context.getParam().getBytes());
    }
  }

  @Test
  public void testRunnableTaskRequestJsonConversion() {
    ArtifactId artifactId = new ArtifactId("test-artifact", new ArtifactVersion("1.0"), ArtifactScope.SYSTEM);
    RunnableTaskRequest request = RunnableTaskRequest.getBuilder("taskClassName").
      withParam("param").
      withNamespace("n1").
      withArtifact(artifactId).
      build();
    Gson gson = new Gson();
    String requestJson = gson.toJson(request);
    RunnableTaskRequest requestFromJson = gson.fromJson(requestJson, RunnableTaskRequest.class);
    Assert.assertEquals(request.getClassName(), requestFromJson.getClassName());
    Assert.assertEquals(request.getNamespace(), requestFromJson.getNamespace());
    Assert.assertEquals(request.getParam(), requestFromJson.getParam());
    Assert.assertEquals(request.getArtifactId(), requestFromJson.getArtifactId());
  }
}
