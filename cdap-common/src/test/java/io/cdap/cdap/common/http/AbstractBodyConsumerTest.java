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

package io.cdap.cdap.common.http;

import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractBodyConsumer}.
 */
public class AbstractBodyConsumerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testChunkAndFinishedWritesAndCleansUp() throws Exception {
    File targetFile = new File(TEMP_FOLDER.newFolder(), "upload.bin");
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    AtomicReference<String> readBack = new AtomicReference<>();

    AbstractBodyConsumer consumer = new AbstractBodyConsumer(targetFile) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) throws Exception {
        readBack.set(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
      }
    };

    consumer.chunk(Unpooled.copiedBuffer("hello ", StandardCharsets.UTF_8), responder);
    consumer.chunk(Unpooled.copiedBuffer("world", StandardCharsets.UTF_8), responder);
    consumer.finished(responder);

    Assert.assertEquals("hello world", readBack.get());
    Assert.assertFalse(targetFile.exists());
  }

  @Test
  public void testFinishedWithZeroChunksAndExceptionPropagation() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    File emptyTarget = new File(TEMP_FOLDER.newFolder(), "empty.bin");
    AtomicBoolean finishCalled = new AtomicBoolean(false);
    AbstractBodyConsumer emptyConsumer = new AbstractBodyConsumer(emptyTarget) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) {
        finishCalled.set(true);
      }
    };
    emptyConsumer.finished(responder);
    Assert.assertTrue(finishCalled.get());

    // Unchecked exception in onFinish must propagate unwrapped and still clean up file.
    File uncheckedFile = TEMP_FOLDER.newFile();
    IllegalArgumentException unchecked = new IllegalArgumentException("bad payload");
    AbstractBodyConsumer uncheckedConsumer = new AbstractBodyConsumer(uncheckedFile) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) {
        throw unchecked;
      }
    };
    uncheckedConsumer.chunk(Unpooled.copiedBuffer("abc", StandardCharsets.UTF_8), responder);
    try {
      uncheckedConsumer.finished(responder);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException caught) {
      Assert.assertSame(unchecked, caught);
    }
    Assert.assertFalse(uncheckedFile.exists());

    // Checked exception in onFinish must wrap in RuntimeException and clean up file.
    File checkedFile = TEMP_FOLDER.newFile();
    IOException ioFailure = new IOException("finish io failure");
    AbstractBodyConsumer checkedConsumer = new AbstractBodyConsumer(checkedFile) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) throws Exception {
        throw ioFailure;
      }
    };
    checkedConsumer.chunk(Unpooled.copiedBuffer("abc", StandardCharsets.UTF_8), responder);
    try {
      checkedConsumer.finished(responder);
      Assert.fail("Expected RuntimeException");
    } catch (RuntimeException caught) {
      Assert.assertSame(ioFailure, caught.getCause());
    }
    Assert.assertFalse(checkedFile.exists());
  }

  @Test
  public void testChunkIoFailureAndHandleErrorCleanup() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    // Passing an existing directory as target file forces FileOutputStream to throw IOException.
    File directoryTarget = TEMP_FOLDER.newFolder();
    AbstractBodyConsumer dirConsumer = new AbstractBodyConsumer(directoryTarget) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) {
        // no-op
      }
    };
    try {
      dirConsumer.chunk(Unpooled.copiedBuffer("data", StandardCharsets.UTF_8), responder);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof IOException);
    }

    // handleError when output != null and when output == null.
    File activeFile = new File(TEMP_FOLDER.newFolder(), "active.bin");
    AtomicReference<Throwable> recordedError = new AtomicReference<>();
    AbstractBodyConsumer errorConsumer = new AbstractBodyConsumer(activeFile) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) {
        // no-op
      }

      @Override
      protected void onError(Throwable cause) {
        recordedError.set(cause);
      }
    };
    errorConsumer.chunk(Unpooled.copiedBuffer("partial", StandardCharsets.UTF_8), responder);
    Assert.assertTrue(activeFile.exists());
    RuntimeException uploadFailure = new RuntimeException("network reset");
    errorConsumer.handleError(uploadFailure);
    Assert.assertSame(uploadFailure, recordedError.get());
    Assert.assertFalse(activeFile.exists());

    // handleError when output == null (before any chunk).
    File uninitializedFile = TEMP_FOLDER.newFile();
    AtomicReference<Throwable> nullOutError = new AtomicReference<>();
    AbstractBodyConsumer nullOutputConsumer = new AbstractBodyConsumer(uninitializedFile) {
      @Override
      protected void onFinish(HttpResponder httpResponder, File file) {
        // no-op
      }

      @Override
      protected void onError(Throwable cause) {
        nullOutError.set(cause);
      }
    };
    nullOutputConsumer.handleError(uploadFailure);
    Assert.assertSame(uploadFailure, nullOutError.get());
    Assert.assertFalse(uninitializedFile.exists());
  }
}
