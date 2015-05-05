/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.http;

import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An abstract implementation of {@link BodyConsumer} that preserves data to a
 * {@link File}. The file will be automatically cleanup when the processing completed.
 */
public abstract class AbstractBodyConsumer extends BodyConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBodyConsumer.class);

  private final File file;
  private OutputStream output;

  protected AbstractBodyConsumer(File file) {
    this.file = file;
  }

  @Override
  public void chunk(ChannelBuffer request, HttpResponder responder) {
    try {
      if (output == null) {
        output = new FileOutputStream(file);
      }
      request.readBytes(output, request.readableBytes());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final void finished(HttpResponder responder) {
    try {
      if (output != null) {
        output.close();
      }
      onFinish(responder, file);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      cleanup();
    }
  }

  @Override
  public final void handleError(Throwable cause) {
    try {
      LOG.error("Failed to handle upload", cause);
      if (output != null) {
        Closeables.closeQuietly(output);
      }
      onError(cause);
      // The netty-http framework will response with 500, no need to response in here.
    } finally {
      cleanup();
    }
  }

  protected abstract void onFinish(HttpResponder responder, File file) throws Exception;

  protected void onError(Throwable cause) {
    // No-op
  }

  private void cleanup() {
    if (file.exists() && !file.delete()) {
      LOG.warn("Failed to cleanup file {}", file);
    }
  }
}
