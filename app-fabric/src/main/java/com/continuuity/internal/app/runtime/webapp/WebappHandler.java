package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Files;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

/**
 * Http service handler that serves files in deployed jar.
 */
public class WebappHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WebappHandler.class);
  private static final int MAX_CACHE_FILE_LENGTH = 5 * 1024 * 1024;
  private static final int MAX_CACHE_WEIGHT = 250 * 1024 * 1024;
  private static final int BUFFER_SIZE = 4096;

  private final File serveDir;
  private final LoadingCache<String, CacheValue> fileCache;

  public WebappHandler(File serveDir) {
    this.serveDir = serveDir;
    this.fileCache = CacheBuilder.newBuilder()
      .maximumWeight(MAX_CACHE_WEIGHT)
      .weigher(new Weigher<String, CacheValue>() {
        @Override
        public int weigh(String key, CacheValue value) {
          return value.getContents().length;
        }
      }).build(new CacheLoader<String, CacheValue>() {
        @Override
        public CacheValue load(String file) throws Exception {
          return new CacheValue(Files.toByteArray(new File(file)), URLConnection.guessContentTypeFromName(file));
        }
      });
  }

  @GET
  @Path("/.*")
  public void serve(HttpRequest request, HttpResponder responder) {
    try {
      File file = new File(serveDir, request.getUri());

      if (request.getUri().equals("/")) {
        file = new File(serveDir, "index.html");
      }

      if (!file.exists()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      if (!file.isFile()) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }

      if (file.length() > MAX_CACHE_FILE_LENGTH) {
        sendChunked(file, responder);
        return;
      }

      CacheValue cacheValue = fileCache.get(file.getAbsolutePath());
      responder.sendByteArray(HttpResponseStatus.OK, cacheValue.getContents(),
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, cacheValue.getContentType()));

    } catch (Throwable t) {
      LOG.error("Got exception: ", t);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @GET
  @Path("/status")
  public void status(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK\n");
  }

  private void sendChunked(File file, HttpResponder responder) throws IOException {
    InputStream in = new BufferedInputStream(new FileInputStream(file));

    try {
      byte [] bytes = new byte[BUFFER_SIZE];

      responder.sendChunkStart(HttpResponseStatus.OK,
                               ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                    URLConnection.guessContentTypeFromName(file.getAbsolutePath())));
      int len;
      while ((len = in.read(bytes, 0, BUFFER_SIZE)) != -1) {
        responder.sendChunk(ChannelBuffers.wrappedBuffer(bytes, 0, len));
      }

      responder.sendChunkEnd();
    } finally {
      in.close();
    }
  }

  private static class CacheValue {
    private final byte [] contents;
    private final String contentType;

    private CacheValue(byte[] contents, String contentType) {
      this.contents = contents;
      this.contentType = contentType;
    }

    public byte[] getContents() {
      return contents;
    }

    public String getContentType() {
      return contentType;
    }
  }
}
