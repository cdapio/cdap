/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.client.app.DatasetWriterService;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.PingService;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramResult;
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.BatchProgramStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link ProgramClient}.
 */
@Category(XSlowTests.class)
public class ProgramClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClientTestRun.class);

  private ApplicationClient appClient;
  private ProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
  }

  @Test
  public void testBatchProgramCalls() throws Exception {
    final NamespaceId namespace = NamespaceId.DEFAULT;
    final ApplicationId appId = namespace.app(FakeApp.NAME);
    BatchProgram pingService = new BatchProgram(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    BatchProgram writerService = new BatchProgram(FakeApp.NAME, ProgramType.SERVICE, DatasetWriterService.NAME);
    BatchProgram missing = new BatchProgram(FakeApp.NAME, ProgramType.SERVICE, "not" + PingService.NAME);

    appClient.deploy(namespace, createAppJarFile(FakeApp.class));
    try {
      // make a batch call to start multiple programs, one of which does not exist
      List<BatchProgramStart> programStarts = ImmutableList.of(
        new BatchProgramStart(pingService),
        new BatchProgramStart(writerService),
        new BatchProgramStart(missing)
      );
      List<BatchProgramResult> results = programClient.start(namespace, programStarts);
      // check that we got a 200 for programs that exist, and a 404 for the one that doesn't
      for (BatchProgramResult result : results) {
        if (missing.getProgramId().equals(result.getProgramId())) {
          Assert.assertEquals(404, result.getStatusCode());
        } else {
          Assert.assertEquals(200, result.getStatusCode());
        }
      }

      // wait for all programs to be in RUNNING status
      assertProgramRuns(programClient, namespace.app(pingService.getAppId()).service(pingService.getProgramId()),
                        ProgramRunStatus.RUNNING, 1, 10);
      assertProgramRuns(programClient, namespace.app(writerService.getAppId()).service(writerService.getProgramId()),
                        ProgramRunStatus.RUNNING, 1, 10);

      // make a batch call for status of programs, one of which does not exist
      List<BatchProgram> programs = ImmutableList.of(pingService, writerService, missing);
      List<BatchProgramStatus> statusList = programClient.getStatus(namespace, programs);
      // check status is running for programs that exist, and that we get a 404 for the one that doesn't
      for (BatchProgramStatus status : statusList) {
        if (missing.getProgramId().equals(status.getProgramId())) {
          Assert.assertEquals(404, status.getStatusCode());
        } else {
          Assert.assertEquals(200, status.getStatusCode());
          Assert.assertEquals("RUNNING", status.getStatus());
        }
      }

      // make a batch call to stop programs, one of which does not exist
      results = programClient.stop(namespace, programs);
      // check that we got a 200 for programs that exist, and a 404 for the one that doesn't
      for (BatchProgramResult result : results) {
        if (missing.getProgramId().equals(result.getProgramId())) {
          Assert.assertEquals(404, result.getStatusCode());
        } else {
          Assert.assertEquals(200, result.getStatusCode());
        }
      }

      // check programs are in stopped state
      final List<BatchProgram> stoppedPrograms = ImmutableList.of(pingService, writerService);
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          List<BatchProgramStatus> statusList = programClient.getStatus(namespace, stoppedPrograms);
          for (BatchProgramStatus status : statusList) {
            if (status.getStatusCode() != 200) {
              return false;
            }
            if (status.getStatus().equals("RUNNING")) {
              return false;
            }
          }
          return true;
        }
      }, 10, TimeUnit.SECONDS);
    } finally {
      try {
        appClient.delete(appId);
      } catch (Exception e) {
        LOG.error("Error deleting app {} during test cleanup.", appId, e);
      }
    }
  }
}
