/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Internal {@link HttpHandler} for Artifact Localizer.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class ArtifactLocalizerHttpHandlerInternal extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
                                                                         new BasicThrowableCodec()).create();
  private final ArtifactLocalizer artifactLocalizer;

  @VisibleForTesting
  public ArtifactLocalizerHttpHandlerInternal(ArtifactLocalizer artifactLocalizer) {
    this.artifactLocalizer = artifactLocalizer;
  }

  @GET
  @Path("/artifact/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void artifact(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("artifact-name") String artifactName,
                       @PathParam("artifact-version") String artifactVersion) {

    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersion);
    try {
      File artifactPath = artifactLocalizer.getAndUnpackArtifact(artifactId);
      responder.sendString(HttpResponseStatus.OK, artifactPath.toString());
    } catch (Exception ex) {
      if (ex instanceof HttpErrorStatusProvider) {
        HttpResponseStatus status = HttpResponseStatus.valueOf(((HttpErrorStatusProvider) ex).getStatusCode());
        responder.sendString(status, exceptionToJson(ex));
      } else {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex));
      }
    }
  }

  @GET
  @Path("/usage")
  public void getServiceUsageMetrics(HttpRequest request, HttpResponder responder) {
    String url = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", "localhost", 11022);
    try {
      JMXServiceURL serviceUrl = new JMXServiceURL(url);
      JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
      MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
      MemoryMXBean memoryMXBean = ManagementFactory
        .newPlatformMXBeanProxy(mbeanConn, ManagementFactory.MEMORY_MXBEAN_NAME,
                                MemoryMXBean.class);
      MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
      String memoryUsage = String
        .format("Max memory is %d. Memory used is %d ", heapMemoryUsage.getMax(), heapMemoryUsage.getUsed());
      responder.sendString(HttpResponseStatus.OK, memoryUsage);
    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(e));
    }
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
