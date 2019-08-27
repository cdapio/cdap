/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.service;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Testing app for {@link ServiceArtifactTestRun}.
 */
public class ServiceArtifactApp extends AbstractApplication {

  @Override
  public void configure() {
    addService("artifact", new ArtifactHandler());
  }

  /**
   * Test handler.
   */
  public static final class ArtifactHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/list")
    public void list(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      responder.sendJson(getContext().listArtifacts());
    }

    @GET
    @Path("/load")
    public void load(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("parent") String parent,
                     @QueryParam("plugin") String plugin,
                     @QueryParam("class") String className) throws IOException, ClassNotFoundException {
      List<ArtifactInfo> artifacts = getContext().listArtifacts();

      ArtifactInfo parentInfo = artifacts.stream()
        .filter(info -> info.getName().equals(parent))
        .findFirst().orElseThrow(NotFoundException::new);

      try (CloseableClassLoader parentClassLoader = getContext().createClassLoader(parentInfo, null)) {
        ArtifactInfo pluginInfo = artifacts.stream()
          .filter(info -> info.getName().equals(plugin))
          .findFirst().orElseThrow(NotFoundException::new);

        try (CloseableClassLoader pluginClassLoader = getContext().createClassLoader(pluginInfo, parentClassLoader)) {
          Class<?> cls = pluginClassLoader.loadClass(className);

          // Should be loaded with different classloader than this class
          if (Objects.equals(cls.getClassLoader(), getClass().getClassLoader())) {
            throw new IllegalStateException("Incorrect classloader isolation");
          }
          responder.sendString(cls.getName());
        }
      }
    }
  }
}
