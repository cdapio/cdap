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

package io.cdap.cdap.common.internal.remote;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Netty HTTP Handler for the standalone Task Manager Service.
 */
@Path("/v3/taskmanager")
public class TaskManagerHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerHttpHandler.class);
  private static final Gson GSON = new Gson();
  private final TaskManager taskManager = TaskManager.getInstance();

  @POST
  @Path("/resolve")
  public void resolve(FullHttpRequest request, HttpResponder responder) {
    try {
      String jsonBody = request.content().toString(StandardCharsets.UTF_8);
      ResolveRequest resolveRequest = GSON.fromJson(jsonBody, ResolveRequest.class);

      if (resolveRequest == null || resolveRequest.getNamespace() == null || resolveRequest.getPods() == null) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      // Convert serialized pods back to Discoverable objects
      List<Discoverable> discoverables = new ArrayList<>();
      for (PodInfo podInfo : resolveRequest.getPods()) {
        discoverables.add(new Discoverable("task.worker",
            new InetSocketAddress(podInfo.getHost(), podInfo.getPort())));
      }

      Discoverable selectedPod = taskManager.resolvePod(resolveRequest.getNamespace(), discoverables);

      if (selectedPod == null) {
        responder.sendStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
        return;
      }

      PodInfo responsePod = new PodInfo(
          selectedPod.getSocketAddress().getHostString(),
          selectedPod.getSocketAddress().getPort()
      );

      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responsePod));
    } catch (Exception e) {
      LOG.error("Failed to resolve pod in Task Manager Service", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/finish")
  public void finish(FullHttpRequest request, HttpResponder responder) {
    try {
      String jsonBody = request.content().toString(StandardCharsets.UTF_8);
      FinishRequest finishRequest = GSON.fromJson(jsonBody, FinishRequest.class);

      if (finishRequest == null || finishRequest.getNamespace() == null || finishRequest.getPod() == null) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      Discoverable pod = new Discoverable("task.worker",
          new InetSocketAddress(finishRequest.getPod().getHost(), finishRequest.getPod().getPort()));

      taskManager.finishTask(finishRequest.getNamespace(), pod);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Failed to finish task in Task Manager Service", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  // DTO Classes for Serialization
  public static class ResolveRequest {
    private String namespace;
    private List<PodInfo> pods;

    public String getNamespace() {
      return namespace;
    }

    public List<PodInfo> getPods() {
      return pods;
    }
  }

  public static class FinishRequest {
    private String namespace;
    private PodInfo pod;

    public String getNamespace() {
      return namespace;
    }

    public PodInfo getPod() {
      return pod;
    }
  }

  public static class PodInfo {
    private String host;
    private int port;

    public PodInfo(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }
  }
}
