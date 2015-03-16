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

package co.cask.cdap.templates.etl.core;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.templates.etl.api.SinkContext;
import co.cask.cdap.templates.etl.api.SourceContext;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.lib.sinks.realtime.TableSink;
import co.cask.cdap.templates.etl.lib.sources.realtime.MySQLSource;
import co.cask.cdap.templates.etl.lib.transforms.PutTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RealtimeAdapterDriver extends AbstractWorker {

  private static final Logger LOG = LoggerFactory.getLogger(RealtimeAdapterDriver.class);
  private static final Type TYPE = new TypeToken<SourceState>() { }.getType();
  private volatile boolean running;

  private RealtimeSource source;
  private RealtimeSink sink;
  private Transform transform;

  private DatasetContext dsContext;

  @Override
  public void configure() {
    //TODO: Pain Point: How do we figure out what all datasets the adapter stages might potentially use?
    //One possible way would be invoke configure that gets the name, desc and get all dataset names that each stage uses.
    useDatasets("tweetTable");
    useDatasets("stateStore");
    useDatasets("myTable");
  }

  @Override
  public void initialize(final WorkerContext context) throws Exception {
    super.initialize(context);
//      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//      Class sourceClass = classLoader.loadClass(context.getRuntimeArguments().get("source"));
//      Class transformClass = classLoader.loadClass(context.getRuntimeArguments().get("transform"));
//      Class sinkClass = classLoader.loadClass(context.getRuntimeArguments().get("sink"));

    Class sourceClass = MySQLSource.class;
    Class transformClass = PutTransform.class;
    Class sinkClass = TableSink.class;

    Class sourceGen = (Class) ((ParameterizedType) (sourceClass.getGenericSuperclass())).getActualTypeArguments()[0];
    Class transIn = (Class) ((ParameterizedType) (transformClass.getGenericSuperclass())).getActualTypeArguments()[0];
    Class transOut = (Class) ((ParameterizedType) (transformClass.getGenericSuperclass())).getActualTypeArguments()[1];
    Class sinkIn = (Class) ((ParameterizedType) (sinkClass.getGenericSuperclass())).getActualTypeArguments()[0];

    Preconditions.checkArgument(sourceGen.equals(transIn));
    Preconditions.checkArgument(sinkIn.equals(transOut));

    source = (RealtimeSource) sourceClass.newInstance();
    sink = (RealtimeSink) sinkClass.newInstance();
    transform = (Transform) transformClass.newInstance();

    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        dsContext = datasetContext;
      }
    });

    final TransformContext transformContext = new TransformContext() {
      @Override
      public int getInstanceId() {
        return context.getInstanceId();
      }

      @Override
      public int getInstanceCount() {
        return context.getInstanceCount();
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        //TOOD: Send only transform specific logic.
        return context.getRuntimeArguments();
      }
    };

    final SourceContext sourceContext = new SourceContext() {

      @Override
      public int getInstanceId() {
        return transformContext.getInstanceId();
      }

      @Override
      public int getInstanceCount() {
        return transformContext.getInstanceCount();
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return transformContext.getRuntimeArguments();
      }
    };

    final SinkContext sinkContext = new SinkContext() {
      @Override
      public <T extends Dataset> T getDataset(String s) throws DatasetInstantiationException {
        return dsContext.getDataset(s);
      }

      @Override
      public <T extends Dataset> T getDataset(String s, Map<String, String> map) throws DatasetInstantiationException {
        return dsContext.getDataset(s, map);
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return sourceContext.getRuntimeArguments();
      }

      @Override
      public void write(String s, String s1) throws IOException {
        getContext().write(s, s1);
      }

      @Override
      public void write(String s, String s1, Map<String, String> map) throws IOException {
        getContext().write(s, s1, map);
      }

      @Override
      public void write(String s, ByteBuffer byteBuffer) throws IOException {
        getContext().write(s, byteBuffer);
      }

      @Override
      public void write(String s, StreamEventData streamEventData) throws IOException {
        getContext().write(s, streamEventData);
      }

      @Override
      public void writeFile(String s, File file, String s1) throws IOException {
        getContext().writeFile(s, file, s1);
      }

      @Override
      public StreamBatchWriter createBatchWriter(String s, String s1) throws IOException {
        return getContext().createBatchWriter(s, s1);
      }

      @Override
      public int getInstanceId() {
        return sourceContext.getInstanceId();
      }

      @Override
      public int getInstanceCount() {
        return sourceContext.getInstanceCount();
      }
    };

    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        source.initialize(sourceContext);
        sink.initialize(sinkContext);
        transform.initialize(transformContext);
      }
    });
  }

  @Override
  public void run() {
    running = true;
    final List<Object> sourceList = Lists.newArrayList();
    final List<Object> transformList = Lists.newArrayList();

    while (running) {

      final SourceState currentState = new SourceState();

      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          KeyValueTable stateTable = datasetContext.getDataset("stateStore");
          SourceState oldState = new Gson().fromJson(Bytes.toString(stateTable.read("sourceState")), TYPE);
          if (oldState != null) {
            currentState.setState(oldState.getState());
          }
        }
      });

      final SourceState sourceState = source.poll(new Emitter() {
        @Override
        public void emit(Object obj) {
          sourceList.add(obj);
        }
      }, currentState);

      for(Object o : sourceList) {
        transform.transform(o, new Emitter() {
          @Override
          public void emit(Object obj) {
            transformList.add(obj);
          }
        });
      }

      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          KeyValueTable stateTable = datasetContext.getDataset("stateStore");
          for (Object o : transformList) {
            sink.write(o);
          }
          stateTable.write("sourceState", new Gson().toJson(sourceState, TYPE));
        }
      });

      sourceList.clear();
      transformList.clear();
    }
    source.destroy();
    transform.destroy();
    sink.destroy();
  }

  @Override
  public void stop() {
    running = false;
  }
}
