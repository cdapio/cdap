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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.ClassLoaderCaller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import com.google.common.base.Throwables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Implementation of {@link DataWriter}.
 */
public class DefaultDataWriter implements DataWriter {
  private final WorkerContext context;
  private final DatasetContext dsContext;
  private final Caller caller;

  public DefaultDataWriter(WorkerContext context, DatasetContext dsContext) {
    this.context = context;
    this.dsContext = dsContext;
    this.caller = ClassLoaderCaller.wrap(NoStageLoggingCaller.wrap(Caller.DEFAULT), getClass().getClassLoader());
  }

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return dsContext.getDataset(name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name)
    throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return dsContext.getDataset(namespace, name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name, final Map<String, String> arguments)
    throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return dsContext.getDataset(name, arguments);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return dsContext.getDataset(namespace, name, arguments);
      }
    });
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        dsContext.releaseDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        dsContext.discardDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void write(final String stream, final String data) throws IOException {
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          context.write(stream, data);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void write(final String stream, final String data, final Map<String, String> headers) throws IOException {
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          context.write(stream, data, headers);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void write(final String stream, final ByteBuffer data) throws IOException {
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          context.write(stream, data);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void write(final String stream, final StreamEventData data) throws IOException {
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          context.write(stream, data);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void writeFile(final String stream, final File file, final String contentType) throws IOException {
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          context.writeFile(stream, file, contentType);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public StreamBatchWriter createBatchWriter(final String stream, final String contentType) throws IOException {
    try {
      return caller.call(new Callable<StreamBatchWriter>() {
        @Override
        public StreamBatchWriter call() throws Exception {
          return context.createBatchWriter(stream, contentType);
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }
}
