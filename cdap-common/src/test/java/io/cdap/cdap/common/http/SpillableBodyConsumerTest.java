/*
 * Copyright © 2020-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for {@link SpillableBodyConsumer}.
 */
public class SpillableBodyConsumerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testSpilling() throws Exception {
    testPost(Strings.repeat("0123456789", 10), 15);
  }

  @Test
  public void testNoSpill() throws Exception {
    testPost(Strings.repeat("0123456789", 10), 1024);
  }

  @Test
  public void testFinishedExceptionPropagationAndCleanup() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    // 1. Unchecked RuntimeException in processInput is rethrown unwrapped (no spill).
    java.nio.file.Path noSpillPath = TEMP_FOLDER.newFile().toPath();
    IllegalArgumentException unchecked = new IllegalArgumentException("invalid body");
    SpillableBodyConsumer uncheckedConsumer = new SpillableBodyConsumer(noSpillPath, 1024) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder httpResponder) {
        throw unchecked;
      }
    };
    uncheckedConsumer.chunk(Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8), responder);
    try {
      uncheckedConsumer.finished(responder);
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (IllegalArgumentException caught) {
      Assert.assertSame(unchecked, caught);
    }
    Assert.assertFalse(Files.exists(noSpillPath));

    // 2. Checked IOException in processInput is wrapped in RuntimeException with spill path message.
    java.nio.file.Path spilledPath = TEMP_FOLDER.newFile().toPath();
    IOException checked = new IOException("corrupt stream");
    SpillableBodyConsumer checkedConsumer = new SpillableBodyConsumer(spilledPath, 2) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder httpResponder)
          throws Exception {
        throw checked;
      }
    };
    checkedConsumer.chunk(Unpooled.copiedBuffer("0123456789", StandardCharsets.UTF_8), responder);
    try {
      checkedConsumer.finished(responder);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertSame(checked, caught.getCause());
      Assert.assertTrue(caught.getMessage().contains(spilledPath.toString()));
    }
    Assert.assertFalse(Files.exists(spilledPath));
  }

  @Test
  public void testHandleErrorAndCloseFailureIgnored() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    // 1. handleError when outputStream == null (never spilled).
    java.nio.file.Path unspilledPath = TEMP_FOLDER.newFile().toPath();
    SpillableBodyConsumer unspilled = new SpillableBodyConsumer(unspilledPath, 1024) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder httpResponder) {
        // no-op
      }
    };
    unspilled.chunk(Unpooled.copiedBuffer("tiny", StandardCharsets.UTF_8), responder);
    unspilled.handleError(new RuntimeException("client disconnected"));
    Assert.assertFalse(Files.exists(unspilledPath));

    // 2. finished() when outputStream.close() throws IOException still processes input and cleans up.
    java.nio.file.Path realSpill = TEMP_FOLDER.newFile().toPath();
    AtomicBoolean closeFailed = new AtomicBoolean(false);
    java.nio.file.Path wrappedSpill = wrapPathWithFailingClose(realSpill, closeFailed);
    AtomicReference<String> processedBody = new AtomicReference<>();

    SpillableBodyConsumer closeFailFinish = new SpillableBodyConsumer(wrappedSpill, 4) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder httpResponder)
          throws IOException {
        processedBody.set(
            new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8));
      }
    };
    closeFailFinish.chunk(Unpooled.copiedBuffer("abcdefghij", StandardCharsets.UTF_8), responder);
    closeFailFinish.finished(responder);
    Assert.assertTrue(closeFailed.get());
    Assert.assertEquals("abcdefghij", processedBody.get());
    Assert.assertFalse(Files.exists(realSpill));

    // 3. handleError() when outputStream.close() throws IOException swallows close failure and cleans up.
    java.nio.file.Path realErrorSpill = TEMP_FOLDER.newFile().toPath();
    AtomicBoolean errorCloseFailed = new AtomicBoolean(false);
    java.nio.file.Path wrappedErrorSpill = wrapPathWithFailingClose(
        realErrorSpill, errorCloseFailed);
    SpillableBodyConsumer closeFailError = new SpillableBodyConsumer(wrappedErrorSpill, 4) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder httpResponder) {
        // no-op
      }
    };
    closeFailError.chunk(Unpooled.copiedBuffer("abcdefghij", StandardCharsets.UTF_8), responder);
    closeFailError.handleError(new IOException("upload failed"));
    Assert.assertTrue(errorCloseFailed.get());
    Assert.assertFalse(Files.exists(realErrorSpill));
  }

  private static java.nio.file.Path wrapPathWithFailingClose(java.nio.file.Path delegatePath,
      AtomicBoolean closeFailed) {
    FileSystem delegateFs = delegatePath.getFileSystem();
    FileSystemProvider realProvider = delegateFs.provider();
    FileSystemProvider wrapperProvider = new DelegatingFileSystemProvider(realProvider) {
      @Override
      public OutputStream newOutputStream(java.nio.file.Path path, OpenOption... options)
          throws IOException {
        OutputStream raw = super.newOutputStream(delegatePath, options);
        return new FilterOutputStream(raw) {
          @Override
          public void close() throws IOException {
            super.close();
            closeFailed.set(true);
            throw new IOException("simulated outputStream close failure");
          }
        };
      }

      @Override
      public boolean deleteIfExists(java.nio.file.Path path) throws IOException {
        return super.deleteIfExists(delegatePath);
      }
    };

    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.when(mockFs.provider()).thenReturn(wrapperProvider);

    return (java.nio.file.Path) Proxy.newProxyInstance(
        java.nio.file.Path.class.getClassLoader(),
        new Class<?>[]{java.nio.file.Path.class},
        (proxy, method, args) -> {
          if ("getFileSystem".equals(method.getName()) && method.getParameterCount() == 0) {
            return mockFs;
          }
          return method.invoke(delegatePath, args);
        });
  }

  private void testPost(String body, int bufferLimit) throws Exception {
    NettyHttpService httpService = NettyHttpService.builder("test")
        .setHttpHandlers(new TestHandler(bufferLimit))
        .build();
    httpService.start();
    try {
      InetSocketAddress addr = httpService.getBindAddress();
      URL url = new URL(String.format("http://%s:%d/post", addr.getHostName(), addr.getPort()));
      HttpResponse response = HttpRequests.execute(
          io.cdap.common.http.HttpRequest.post(url).withBody(body).build(),
          new HttpRequestConfig(1000, 10000000));

      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals(body, response.getResponseBodyAsString());
    } finally {
      httpService.stop();
    }
  }

  /**
   * Minimal delegating {@link FileSystemProvider} for injecting OutputStream close failures.
   */
  private abstract static class DelegatingFileSystemProvider extends FileSystemProvider {
    private final FileSystemProvider delegate;

    DelegatingFileSystemProvider(FileSystemProvider delegate) {
      this.delegate = delegate;
    }

    @Override
    public OutputStream newOutputStream(java.nio.file.Path path, OpenOption... options)
        throws IOException {
      return delegate.newOutputStream(path, options);
    }

    @Override
    public boolean deleteIfExists(java.nio.file.Path path) throws IOException {
      return delegate.deleteIfExists(path);
    }

    @Override
    public String getScheme() {
      return delegate.getScheme();
    }

    @Override
    public FileSystem newFileSystem(java.net.URI uri, java.util.Map<String, ?> env)
        throws IOException {
      return delegate.newFileSystem(uri, env);
    }

    @Override
    public FileSystem getFileSystem(java.net.URI uri) {
      return delegate.getFileSystem(uri);
    }

    @Override
    public java.nio.file.Path getPath(java.net.URI uri) {
      return delegate.getPath(uri);
    }

    @Override
    public java.nio.channels.SeekableByteChannel newByteChannel(java.nio.file.Path path,
        java.util.Set<? extends OpenOption> options,
        java.nio.file.attribute.FileAttribute<?>... attrs) throws IOException {
      return delegate.newByteChannel(path, options, attrs);
    }

    @Override
    public java.nio.file.DirectoryStream<java.nio.file.Path> newDirectoryStream(
        java.nio.file.Path dir,
        java.nio.file.DirectoryStream.Filter<? super java.nio.file.Path> filter)
        throws IOException {
      return delegate.newDirectoryStream(dir, filter);
    }

    @Override
    public void createDirectory(java.nio.file.Path dir,
        java.nio.file.attribute.FileAttribute<?>... attrs) throws IOException {
      delegate.createDirectory(dir, attrs);
    }

    @Override
    public void delete(java.nio.file.Path path) throws IOException {
      delegate.delete(path);
    }

    @Override
    public void copy(java.nio.file.Path source, java.nio.file.Path target,
        java.nio.file.CopyOption... options) throws IOException {
      delegate.copy(source, target, options);
    }

    @Override
    public void move(java.nio.file.Path source, java.nio.file.Path target,
        java.nio.file.CopyOption... options) throws IOException {
      delegate.move(source, target, options);
    }

    @Override
    public boolean isSameFile(java.nio.file.Path path, java.nio.file.Path path2)
        throws IOException {
      return delegate.isSameFile(path, path2);
    }

    @Override
    public boolean isHidden(java.nio.file.Path path) throws IOException {
      return delegate.isHidden(path);
    }

    @Override
    public java.nio.file.FileStore getFileStore(java.nio.file.Path path) throws IOException {
      return delegate.getFileStore(path);
    }

    @Override
    public void checkAccess(java.nio.file.Path path, java.nio.file.AccessMode... modes)
        throws IOException {
      delegate.checkAccess(path, modes);
    }

    @Override
    public <V extends java.nio.file.attribute.FileAttributeView> V getFileAttributeView(
        java.nio.file.Path path, Class<V> type, java.nio.file.LinkOption... options) {
      return delegate.getFileAttributeView(path, type, options);
    }

    @Override
    public <A extends java.nio.file.attribute.BasicFileAttributes> A readAttributes(
        java.nio.file.Path path, Class<A> type, java.nio.file.LinkOption... options)
        throws IOException {
      return delegate.readAttributes(path, type, options);
    }

    @Override
    public java.util.Map<String, Object> readAttributes(java.nio.file.Path path, String attributes,
        java.nio.file.LinkOption... options) throws IOException {
      return delegate.readAttributes(path, attributes, options);
    }

    @Override
    public void setAttribute(java.nio.file.Path path, String attribute, Object value,
        java.nio.file.LinkOption... options) throws IOException {
      delegate.setAttribute(path, attribute, value, options);
    }
  }

  /**
   * Handler for testing only.
   */
  public static final class TestHandler extends AbstractHttpHandler {

    private final int bufferSize;

    public TestHandler(int bufferSize) {
      this.bufferSize = bufferSize;
    }

    @POST
    @Path("/post")
    public BodyConsumer post(HttpRequest request, HttpResponder responder) throws IOException {
      return new SpillableBodyConsumer(TEMP_FOLDER.newFile().toPath(), bufferSize) {
        @Override
        protected void processInput(InputStream is, HttpResponder responder) throws IOException {
          responder.sendByteArray(HttpResponseStatus.OK, ByteStreams.toByteArray(is),
              new DefaultHttpHeaders());
        }
      };
    }
  }
}
