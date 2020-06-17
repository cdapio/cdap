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

package io.cdap.cdap.internal.app.preview;

import com.google.gson.JsonElement;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Tests for {@link DefaultPreviewRequestQueue}
 */
public class DefaultPreviewRequestQueueTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private PreviewRequestQueue previewRequestQueue;

  @Before
  public void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Preview.WAITING_QUEUE_CAPACITY, 2);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, new Configuration()),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(PreviewStore.class).to(MockPreviewStore.class);
        }
      }
    );

    previewRequestQueue = injector.getInstance(DefaultPreviewRequestQueue.class);
  }

  static class MockPreviewStore implements PreviewStore {
    @Override
    public void put(ApplicationId applicationId, String tracerName, String propertyName, Object value) {

    }

    @Override
    public Map<String, List<JsonElement>> get(ApplicationId applicationId, String tracerName) {
      return null;
    }

    @Override
    public void remove(ApplicationId applicationId) {

    }

    @Override
    public void setProgramId(ProgramRunId programRunId) {

    }

    @Nullable
    @Override
    public ProgramRunId getProgramRunId(ApplicationId applicationId) {
      return null;
    }

    @Override
    public void setPreviewStatus(ApplicationId applicationId, PreviewStatus previewStatus) {

    }

    @Nullable
    @Override
    public PreviewStatus getPreviewStatus(ApplicationId applicationId) {
      return null;
    }

    @Override
    public void add(ApplicationId applicationId, AppRequest appRequest) {

    }

    @Override
    public List<PreviewRequest> getAllInWaitingState() {
      return Collections.emptyList();
    }

    @Override
    public void setPreviewRequestPollerInfo(ApplicationId applicationId, byte[] pollerInfo) {

    }

    @Nullable
    @Override
    public byte[] getPreviewRequestPollerInfo(ApplicationId applicationId) {
      return new byte[0];
    }
  }

  @Test
  public void testPreviewRequestQueue() {
    PreviewConfig previewConfig = new PreviewConfig("WordCount", ProgramType.WORKFLOW, null, null);
    AppRequest<?> testRequest = new AppRequest<>(new ArtifactSummary("test", "1.0"), null, previewConfig);

    byte[] pollerInfo = Bytes.toBytes("runner-1");
    Optional<PreviewRequest> requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertFalse(requestOptional.isPresent());

    ApplicationId app1 = new ApplicationId("default", RunIds.generate().getId());
    PreviewRequest request = new PreviewRequest(app1, testRequest);
    previewRequestQueue.add(request);

    requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertTrue(requestOptional.isPresent());
    request = requestOptional.get();
    ProgramId programId1 = new ProgramId(app1, ProgramType.WORKFLOW, "WordCount");
    Assert.assertEquals(programId1, request.getProgram());

    requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertFalse(requestOptional.isPresent());

    ApplicationId app2 = new ApplicationId("default", RunIds.generate().getId());
    request = new PreviewRequest(app2, testRequest);
    previewRequestQueue.add(request);
    Assert.assertEquals(0, previewRequestQueue.positionOf(app2));

    ApplicationId app3 = new ApplicationId("default", RunIds.generate().getId());
    request = new PreviewRequest(app3, testRequest);
    previewRequestQueue.add(request);
    Assert.assertEquals(1, previewRequestQueue.positionOf(app3));

    ApplicationId app4 = new ApplicationId("default", RunIds.generate().getId());
    request = new PreviewRequest(app4, testRequest);
    boolean exceptionThrown = false;
    try {
      previewRequestQueue.add(request);
    } catch (IllegalStateException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);

    requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertTrue(requestOptional.isPresent());
    request = requestOptional.get();
    ProgramId programId2 = new ProgramId(app2, ProgramType.WORKFLOW, "WordCount");
    Assert.assertEquals(programId2, request.getProgram());

    requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertTrue(requestOptional.isPresent());
    request = requestOptional.get();
    ProgramId programId3 = new ProgramId(app3, ProgramType.WORKFLOW, "WordCount");
    Assert.assertEquals(programId3, request.getProgram());

    requestOptional = previewRequestQueue.poll(pollerInfo);
    Assert.assertFalse(requestOptional.isPresent());
  }
}

