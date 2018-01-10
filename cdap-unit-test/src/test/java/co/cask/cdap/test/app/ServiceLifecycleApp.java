/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.service.ServiceLifeCycleTestRun;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * An application for testing service lifecycle by the {@link ServiceLifeCycleTestRun}.
 */
public class ServiceLifecycleApp extends AbstractApplication {

  public static final String HANDLER_TABLE_NAME = "HandlerTable";

  @Override
  public void configure() {
    addService("test", new TestHandler());

    createDataset(HANDLER_TABLE_NAME, KeyValueTable.class,
                  TableProperties.builder()
                    .setReadlessIncrementSupport(true)
                    .setConflictDetection(ConflictDetection.NONE)
                    .build());
  }

  /**
   * Handler for testing that tracks lifecycle method calls through a static list.
   */
  public static final class TestHandler extends AbstractHttpServiceHandler {

    private static final Queue<ImmutablePair<Integer, String>> STATES = new ConcurrentLinkedQueue<>();

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      STATES.add(ImmutablePair.of(System.identityHashCode(this), "INIT"));
    }

    @GET
    @Path("/states")
    public void getStates(HttpServiceRequest request, HttpServiceResponder responder) {
      // Returns the current states
      responder.sendJson(new ArrayList<>(STATES));
    }

    @PUT
    @Path("/upload")
    public HttpContentConsumer upload(HttpServiceRequest request, HttpServiceResponder responder) {
      return new HttpContentConsumer() {
        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          // No-op
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.sendStatus(200);
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          responder.sendString(500, failureCause.getMessage(), Charsets.UTF_8);
        }
      };
    }

    @GET
    @Path("/download")
    public void download(HttpServiceRequest request, HttpServiceResponder responder) {
      // Responds with a content producer
      KeyValueTable table = getContext().getDataset(HANDLER_TABLE_NAME);
      responder.send(200, new DownloadHttpContentProducer(table), "text/plain");
    }

    @POST
    @Path("/uploadDownload")
    public HttpContentConsumer uploadDownload(HttpServiceRequest request, HttpServiceResponder responder) {
      // Consume request with content consumer, then response with content producer
      return new HttpContentConsumer() {
        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          // no-op
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          KeyValueTable table = getContext().getDataset(HANDLER_TABLE_NAME);
          responder.send(200, new DownloadHttpContentProducer(table), "text/plain");
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          responder.sendString(500, failureCause.getMessage(), Charsets.UTF_8);
        }
      };
    }

    @PUT
    @Path("/invalid")
    public HttpContentConsumer invalidPut(HttpServiceRequest request, HttpServiceResponder responder) {
      // Create a closure on the handler responder
      final HttpServiceResponder finalResponder = responder;
      return new HttpContentConsumer() {
        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          // no-op
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          // Use the closure responder instead of the given responder, it should fail
          finalResponder.sendStatus(200);
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          responder.sendString(500, failureCause.getMessage(), Charsets.UTF_8);
        }
      };
    }

    @GET
    @Path("/invalid")
    public void invalidGet(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("methods") final Set<String> methods) {
      final Queue<ByteBuffer> chunks = new LinkedList<>();
      chunks.add(Charsets.UTF_8.encode("0123456789"));
      chunks.add(ByteBuffer.allocate(0));

      responder.send(200, new HttpContentProducer() {
        @Override
        public long getContentLength() {
          if (methods.contains("getContentLength")) {
            throw new RuntimeException("Exception in getContentLength");
          }
          return -1L;
        }

        @Override
        public ByteBuffer nextChunk(Transactional transactional) throws Exception {
          if (methods.contains("nextChunk")) {
            throw new Exception("Exception in nextChunk");
          }
          return chunks.poll();
        }

        @Override
        public void onFinish() throws Exception {
          if (methods.contains("onFinish")) {
            throw new Exception("Exception in onFinish");
          }
        }

        @Override
        public void onError(Throwable failureCause) {
          if (methods.contains("onError")) {
            throw new RuntimeException("Exception in onError");
          }
        }
      }, "text/plain");
    }

    @Override
    public void destroy() {
      STATES.add(ImmutablePair.of(System.identityHashCode(this), "DESTROY"));
    }

    /**
     * A {@link HttpContentProducer} that will keep producing "0" until it reads a flag from a dataset
     */
    private static final class DownloadHttpContentProducer extends HttpContentProducer {

      private static final Logger LOG = LoggerFactory.getLogger(DownloadHttpContentProducer.class);

      private final KeyValueTable table;
      private long sleepMs;

      private DownloadHttpContentProducer(KeyValueTable table) {
        this.table = table;
      }

      @Override
      public ByteBuffer nextChunk(Transactional transactional) throws Exception {
        final AtomicBoolean completed = new AtomicBoolean();
        transactional.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            byte[] value = table.read("completed");
            completed.set((!(value == null || value.length != 1)) && Bytes.toBoolean(value));
            table.increment(Bytes.toBytes("called"), 1L);
          }
        });
        // Introduce a short delay after sending the first block to avoid sending too much
        TimeUnit.MILLISECONDS.sleep(sleepMs);
        sleepMs = 100L;
        return completed.get() ? ByteBuffer.allocate(0) : Charsets.UTF_8.encode("0");
      }

      @Override
      public void onFinish() throws Exception {
        // no-op
      }

      @Override
      public void onError(Throwable failureCause) {
        LOG.error("Failure: {}", failureCause.getMessage(), failureCause);
      }
    }
  }
}
