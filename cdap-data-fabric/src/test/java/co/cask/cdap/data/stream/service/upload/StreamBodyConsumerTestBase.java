/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service.upload;

import co.cask.cdap.proto.Id;
import co.cask.common.io.ByteBufferInputStream;
import co.cask.http.AbstractHttpResponder;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test base for various stream {@link BodyConsumer} that supports the stream batch endpoint.
 */
public abstract class StreamBodyConsumerTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected abstract ContentInfo generateFile(int recordCount) throws IOException;

  protected abstract BodyConsumer createBodyConsumer(ContentWriterFactory contentWriterFactory);

  @Test
  public void testChunkedContent() throws Exception {
    int recordCount = 1000;
    // Generate a file for upload
    ContentInfo contentInfo = generateFile(recordCount);

    final Map<String, String> contentHeaders = Maps.newHashMap();
    final TestContentWriter contentWriter = new TestContentWriter();
    BodyConsumer bodyConsumer = createBodyConsumer(new ContentWriterFactory() {
      @Override
      public Id.Stream getStream() {
        return Id.Stream.from("test-namespace", "test-stream");
      }

      @Override
      public ContentWriter create(Map<String, String> headers) throws IOException {
        contentHeaders.putAll(headers);
        return contentWriter;
      }
    });
    TestHttpResponder responder = new TestHttpResponder();

    // Feed the file content in small chunk
    sendChunks(contentInfo.getContentSupplier(), 10, bodyConsumer, responder);

    // Verify the processing is completed correctly
    Assert.assertTrue(contentWriter.waitForClose(5, TimeUnit.SECONDS));
    Assert.assertEquals(HttpResponseStatus.OK, responder.getResponseStatus());
    Assert.assertEquals(recordCount, contentWriter.getEvents());
    Assert.assertTrue(contentInfo.verify(contentHeaders, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return new ByteBufferInputStream(contentWriter.getContent().duplicate());
      }
    }));
  }

  /**
   * Sends content as provided by the given {@link InputSupplier} as small chunks to the given
   * {@link BodyConsumer}.
   */
  private void sendChunks(InputSupplier<? extends InputStream> inputSupplier, int chunkSize,
                          BodyConsumer bodyConsumer, HttpResponder responder) throws IOException {
    InputStream input = inputSupplier.getInput();
    try {
      byte[] bytes = new byte[chunkSize];
      int len = input.read(bytes);
      while (len >= 0) {
        bodyConsumer.chunk(ChannelBuffers.copiedBuffer(bytes, 0, len), responder);
        len = input.read(bytes);
      }
      bodyConsumer.finished(responder);
    } catch (Exception e) {
      bodyConsumer.handleError(e);
    } finally {
      input.close();
    }
  }

  /**
   * An interface for sub-classes to implement to provide information about content being provided to body consumer.
   */
  protected interface ContentInfo {
    /**
     * Returns an {@link InputSupplier} that provides the content.
     */
    InputSupplier<? extends InputStream> getContentSupplier();

    /**
     * Verify the uploaded content after being handled by the body consumer
     * created by the {@link #createBodyConsumer(ContentWriterFactory)} method.
     */
    boolean verify(Map<String, String> headers,
                   InputSupplier<? extends InputStream> contentSupplier) throws IOException;
  }

  /**
   * An implementation of {@link ContentInfo} that reads content from the given file.
   */
  protected abstract class FileContentInfo implements ContentInfo {

    private final File file;

    protected FileContentInfo(File file) {
      this.file = file;
    }

    @Override
    public InputSupplier<? extends InputStream> getContentSupplier() {
      return Files.newInputStreamSupplier(file);
    }
  }

  /**
   * A {@link ContentWriter} for testing. It keeps all content written in memory.
   */
  private static class TestContentWriter implements ContentWriter {
    private final List<ChannelBuffer> contents;
    private final CountDownLatch completion;
    private int events;

    public TestContentWriter() {
      this.contents = Lists.newLinkedList();
      this.completion = new CountDownLatch(1);
    }

    @Override
    public void append(ByteBuffer body, boolean immutable) throws IOException {
      contents.add(immutable ? ChannelBuffers.wrappedBuffer(body) : ChannelBuffers.copiedBuffer(body));
      events++;
    }

    @Override
    public void appendAll(Iterator<ByteBuffer> bodies, boolean immutable) throws IOException {
      while (bodies.hasNext()) {
        append(bodies.next(), immutable);
      }
    }

    @Override
    public void close() throws IOException {
      completion.countDown();
    }

    public boolean waitForClose(long timeout, TimeUnit unit) throws InterruptedException {
      return completion.await(timeout, unit);
    }

    public ByteBuffer getContent() {
      return ChannelBuffers.wrappedBuffer(contents.toArray(new ChannelBuffer[contents.size()])).toByteBuffer();
    }

    @Override
    public void cancel() {
      contents.clear();
      completion.countDown();
    }

    public int getEvents() {
      return events;
    }
  }

  /**
   * A {@link HttpResponder} for testing. It only saved the first response status event sent.
   */
  private static class TestHttpResponder extends AbstractHttpResponder {
    private final AtomicReference<HttpResponseStatus> responseStatus = new AtomicReference<HttpResponseStatus>();

    @Override
    public ChunkResponder sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
      // Not used in test
      return null;
    }

    @Override
    public void sendContent(HttpResponseStatus status, ChannelBuffer content,
                            String contentType, Multimap<String, String> headers) {
      responseStatus.compareAndSet(null, status);
    }

    @Override
    public void sendFile(File file, Multimap<String, String> headers) {
      // Not used in test
    }

    public HttpResponseStatus getResponseStatus() {
      return responseStatus.get();
    }
  }
}
