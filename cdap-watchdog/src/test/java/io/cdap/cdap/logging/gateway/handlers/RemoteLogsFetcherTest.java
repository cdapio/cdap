/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link RemoteLogsFetcher}.
 */
public class RemoteLogsFetcherTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testWriteProgramAndSystemLogsClosesChannelQuietly() throws Exception {
    NettyHttpService httpService = NettyHttpService.builder("remote-logs-fetcher-test")
        .setHost("127.0.0.1")
        .setPort(0)
        .setHttpHandlers(new MockLogsHttpHandler())
        .build();
    httpService.start();
    try {
      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      discoveryService.register(
          new Discoverable(Constants.Service.LOGSAVER, httpService.getBindAddress()));
      RemoteClientFactory factory = new RemoteClientFactory(
          discoveryService, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
      RemoteLogsFetcher fetcher = new RemoteLogsFetcher(factory);

      File programLogFile = TMP_FOLDER.newFile("program.log");
      ProgramId programId = NamespaceId.DEFAULT.app("myApp").program(ProgramType.WORKER, "w1");
      fetcher.writeProgramRunLogs(programId, "run-42", 10L, 20L, programLogFile);
      String programContent =
          new String(Files.readAllBytes(programLogFile.toPath()), StandardCharsets.UTF_8);
      Assert.assertEquals("prog:default:myApp:workers:w1:run-42:10:20", programContent);

      File systemLogFile = TMP_FOLDER.newFile("system.log");
      fetcher.writeSystemServiceLog("services", "appfabric", 100L, 200L, systemLogFile);
      String systemContent =
          new String(Files.readAllBytes(systemLogFile.toPath()), StandardCharsets.UTF_8);
      Assert.assertEquals("sys:services:appfabric:100:200", systemContent);
    } finally {
      httpService.stop();
    }

    // Connection failure after httpService stops also closes FileChannel cleanly and throws IOException
    InMemoryDiscoveryService deadDiscovery = new InMemoryDiscoveryService();
    deadDiscovery.register(
        new Discoverable(Constants.Service.LOGSAVER, httpService.getBindAddress()));
    RemoteLogsFetcher disconnectedFetcher = new RemoteLogsFetcher(new RemoteClientFactory(
        deadDiscovery, new DefaultInternalAuthenticator(new AuthenticationTestContext())));
    File disconnectedFile = TMP_FOLDER.newFile("disconnected.log");
    try {
      disconnectedFetcher.writeSystemServiceLog("services", "appfabric", 0L, 1L, disconnectedFile);
      Assert.fail("Expected IOException when remote server is stopped");
    } catch (IOException expected) {
      Assert.assertEquals(0L, disconnectedFile.length());
    }
  }

  /**
   * Test HTTP handler serving program and system logs.
   */
  public static final class MockLogsHttpHandler extends AbstractHttpHandler {

    @GET
    @Path("/v3/namespaces/{ns}/apps/{app}/{type}/{prog}/runs/{run}/logs")
    public void getProgramLogs(HttpRequest request, HttpResponder responder,
        @PathParam("ns") String ns,
        @PathParam("app") String app,
        @PathParam("type") String type,
        @PathParam("prog") String prog,
        @PathParam("run") String run,
        @QueryParam("start") long start,
        @QueryParam("stop") long stop) {
      String body = String.format("prog:%s:%s:%s:%s:%s:%d:%d",
          ns, app, type, prog, run, start, stop);
      responder.sendString(HttpResponseStatus.OK, body, new DefaultHttpHeaders());
    }

    @GET
    @Path("/v3/system/{comp}/{svc}/logs")
    public void getSystemLogs(HttpRequest request, HttpResponder responder,
        @PathParam("comp") String comp,
        @PathParam("svc") String svc,
        @QueryParam("start") long start,
        @QueryParam("stop") long stop) {
      if ("fail".equals(svc)) {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "boom");
        return;
      }
      String body = String.format("sys:%s:%s:%d:%d", comp, svc, start, stop);
      responder.sendString(HttpResponseStatus.OK, body, new DefaultHttpHeaders());
    }
  }
}
