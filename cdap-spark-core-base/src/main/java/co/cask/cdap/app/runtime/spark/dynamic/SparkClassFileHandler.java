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
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A {@link HttpHandler} to serve class file to Spark executor.
 */
public final class SparkClassFileHandler extends AbstractHttpHandler implements URLAdder {

  private static final Logger LOG = LoggerFactory.getLogger(SparkClassFileHandler.class);

  private final ReadWriteLock lock;
  private final Set<URL> urls;
  private URLClassLoader jarClassFinder;

  public SparkClassFileHandler() {
    this.lock = new ReentrantReadWriteLock();
    this.urls = new LinkedHashSet<>();
    this.jarClassFinder = new URLClassLoader(new URL[0], null);
  }

  @Override
  public void destroy(HandlerContext context) {
    Lock writeLock = this.lock.writeLock();
    writeLock.lock();
    try {
      Closeables.closeQuietly(jarClassFinder);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Adds a set of URLs to the class file search path.
   */
  @Override
  public void addURLs(Seq<URL> urls) {
    Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      this.urls.addAll(JavaConversions.asJavaCollection(urls));
      Closeables.closeQuietly(jarClassFinder);
      jarClassFinder = new URLClassLoader(this.urls.toArray(new URL[this.urls.size()]), null);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Removes a set of jar files. Only the last addition of each file in the underly class finder will be removed.
   */
  public void removeURLs(Iterable<? extends URL> urls) {

    Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      for (URL url : urls) {
        this.urls.remove(url);
      }
      Closeables.closeQuietly(jarClassFinder);
      jarClassFinder = new URLClassLoader(this.urls.toArray(new URL[this.urls.size()]), null);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * REST endpoint for the Spark executor nodes to load classes remotely.
   */
  @GET
  @Path("/.*")
  public void getClassFile(HttpRequest request, HttpResponder responder) throws Exception {
    String requestURI = URLDecoder.decode(request.uri(), "UTF-8");
    requestURI = requestURI.isEmpty() ? requestURI : requestURI.substring(1);

    Lock readLock = this.lock.readLock();
    readLock.lock();
    try {
      // Use the classloader to find the class file
      URL resource = jarClassFinder.getResource(requestURI);
      if (resource != null) {
        responder.sendByteArray(HttpResponseStatus.OK, Resources.toByteArray(resource), EmptyHttpHeaders.INSTANCE);
        return;
      }

      throw new NotFoundException(requestURI);
    } finally {
      readLock.unlock();
    }
  }
}
