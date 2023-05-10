/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.tethering.runtime.spi.runtimejob.TetheringRuntimeJobManager;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TetheringAgentServiceTest {

  private TetheringAgentService tetheringAgentService;
  private ProgramStateWriter programStateWriter;
  private ProgramRunInfo programRunInfo;
  private PeerInfo peerInfo;
  @Before
  public void setup() {
    CConfiguration cConf = CConfiguration.create();
    programStateWriter = Mockito.mock(ProgramStateWriter.class);
    tetheringAgentService = new TetheringAgentService(cConf,
        Mockito.mock(TetheringStore.class),
        programStateWriter,
        Mockito.mock(MessagingService.class),
        Mockito.mock(RemoteAuthenticator.class),
        Mockito.mock(LocationFactory.class),
        Mockito.mock(ProvisionerNotifier.class),
        Mockito.mock(NamespaceQueryAdmin.class));

    programRunInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("ns")
            .setApplication("app")
            .setVersion("1.0")
            .setProgramType("WORKFLOW")
            .setProgram("program")
            .setRun("runId")
            .build();

    Map<String, String> metadata = ImmutableMap.of("project", "proj",
        "location", "us-west1");
    List<NamespaceAllocation> namespaces = Collections.singletonList(new NamespaceAllocation("default",
        null, null));
    PeerMetadata peerMetadata = new PeerMetadata(namespaces, metadata, "");
    peerInfo = new PeerInfo("my-inst", "http://my.com/api",
        TetheringStatus.ACCEPTED, peerMetadata, System.currentTimeMillis());
  }

  @Test
  public void testStopProgram() throws IOException {
    TetheringControlMessage controlMessage = TetheringRuntimeJobManager
        .createProgramTerminatePayload(programRunInfo, TetheringControlMessage.Type.STOP_PROGRAM);
    tetheringAgentService.processControlMessage(controlMessage, peerInfo);
    ProgramRunId programRunId = new ProgramRunId(programRunInfo);
    Mockito.verify(programStateWriter, Mockito.times(1)).stop(programRunId, Integer.MAX_VALUE);
  }

  @Test
  public void testStopProgramKill() throws IOException {
    TetheringControlMessage controlMessage = TetheringRuntimeJobManager
        .createProgramTerminatePayload(programRunInfo, TetheringControlMessage.Type.KILL_PROGRAM);
    tetheringAgentService.processControlMessage(controlMessage, peerInfo);
    ProgramRunId programRunId = new ProgramRunId(programRunInfo);
    Mockito.verify(programStateWriter, Mockito.times(1)).stop(programRunId, 0);
  }
}
