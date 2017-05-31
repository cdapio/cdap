/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.dynamic;

import co.cask.cdap.common.NotFoundException;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A {@link HttpHandler} to serve class file to Spark executor.
 */
public final class SparkClassFileHandler extends AbstractHttpHandler {

  private final Set<File> classDirs;

  public SparkClassFileHandler() {
    this.classDirs = Collections.synchronizedSet(new LinkedHashSet<File>());
  }

  /**
   * Adds a directory to the class file search path.
   */
  public void addClassDir(File classDir) {
    classDirs.add(classDir);
  }

  /**
   * Removes a directory to the class file search path.
   */
  public void removeClassDir(File classDir) {
    classDirs.remove(classDir);
  }

  @GET
  @Path("/.*")
  public void getClassFile(HttpRequest request, HttpResponder responder) throws Exception {
    String requestURI = URLDecoder.decode(request.getUri(), "UTF-8");
    requestURI = requestURI.isEmpty() ? requestURI : requestURI.substring(1);

    for (File dir : classDirs) {
      File classFile = new File(dir, requestURI);
      if (classFile.isFile()) {
        responder.sendByteArray(HttpResponseStatus.OK, Files.readAllBytes(classFile.toPath()),
                                ImmutableMultimap.of("Content-Type", "application/octet-stream"));
        return;
      }
    }
    throw new NotFoundException(requestURI);
  }
}
