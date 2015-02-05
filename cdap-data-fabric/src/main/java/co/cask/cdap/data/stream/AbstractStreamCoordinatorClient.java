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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.async.ExecutorUtils;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PropertyChangeListener;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.conf.PropertyUpdater;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Base implementation for {@link StreamCoordinatorClient}.
 */
public abstract class AbstractStreamCoordinatorClient extends AbstractIdleService implements StreamCoordinatorClient {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCoordinatorClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // Executor for performing update action asynchronously
  private final Executor updateExecutor;
  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final Supplier<PropertyStore<StreamProperty>> propertyStore;

  protected AbstractStreamCoordinatorClient(CConfiguration cConf, StreamAdmin streamAdmin) {
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;

    propertyStore = Suppliers.memoize(new Supplier<PropertyStore<StreamProperty>>() {
      @Override
      public PropertyStore<StreamProperty> get() {
        return createPropertyStore(new StreamPropertyCodec());
      }
    });

    // Update action should be infrequent, hence just use an executor that create a new thread everytime.
    updateExecutor = ExecutorUtils.newThreadExecutor(Threads.createDaemonThreadFactory("stream-coordinator-update-%d"));
  }

  /**
   * Creates a {@link PropertyStore}.
   *
   * @param codec Codec for the property stored in the property store
   * @param <T> Type of the property
   * @return A new {@link PropertyStore}.
   */
  protected abstract <T> PropertyStore<T> createPropertyStore(Codec<T> codec);

  @Override
  public ListenableFuture<Integer> nextGeneration(final StreamConfig streamConfig, final int lowerBound) {
    return Futures.transform(propertyStore.get().update(streamConfig.getName(), new PropertyUpdater<StreamProperty>() {
      @Override
      public ListenableFuture<StreamProperty> apply(@Nullable final StreamProperty property) {
        final SettableFuture<StreamProperty> resultFuture = SettableFuture.create();
        updateExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              long currentTTL = (property == null) ? streamConfig.getTTL() : property.getTTL();
              int newGeneration = ((property == null) ? lowerBound : property.getGeneration()) + 1;
              int newThreshold = (property == null) ?
                streamConfig.getNotificationThresholdMB() :
                property.getThreshold();
              // Create the generation directory
              Locations.mkdirsIfNotExists(StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                               newGeneration));
              resultFuture.set(new StreamProperty(newGeneration, currentTTL, newThreshold));
            } catch (IOException e) {
              resultFuture.setException(e);
            }
          }
        });
        return resultFuture;
      }
    }), new Function<StreamProperty, Integer>() {
      @Override
      public Integer apply(StreamProperty property) {
        return property.getGeneration();
      }
    });
  }

  @Override
  public ListenableFuture<Long> changeTTL(final String streamName, final long newTTL) {
    return Futures.transform(propertyStore.get().update(streamName, new PropertyUpdater<StreamProperty>() {
      @Override
      public ListenableFuture<StreamProperty> apply(@Nullable final StreamProperty property) {
        final SettableFuture<StreamProperty> resultFuture = SettableFuture.create();
        updateExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              StreamConfig streamConfig = streamAdmin.getConfig(streamName);
              int currentGeneration = (property == null) ?
                StreamUtils.getGeneration(streamConfig) :
                property.getGeneration();
              int currentThreshold = (property == null) ?
                streamConfig.getNotificationThresholdMB() :
                property.getThreshold();
              resultFuture.set(new StreamProperty(currentGeneration, newTTL, currentThreshold));
            } catch (IOException e) {
              resultFuture.setException(e);
            }
          }
        });
        return resultFuture;
      }
    }), new Function<StreamProperty, Long>() {
      @Override
      public Long apply(StreamProperty property) {
        return property.getTTL();
      }
    });
  }

  @Override
  public ListenableFuture<Integer> changeThreshold(final String streamName, final int newThreshold) {
    return Futures.transform(propertyStore.get().update(streamName, new PropertyUpdater<StreamProperty>() {
      @Override
      public ListenableFuture<StreamProperty> apply(@Nullable final StreamProperty property) {
        final SettableFuture<StreamProperty> resultFuture = SettableFuture.create();
        updateExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              StreamConfig streamConfig = streamAdmin.getConfig(streamName);
              int currentGeneration = (property == null) ?
                StreamUtils.getGeneration(streamConfig) :
                property.getGeneration();
              long currentTTL = (property == null) ?
                streamConfig.getTTL() :
                property.getTTL();
              resultFuture.set(new StreamProperty(currentGeneration, currentTTL, newThreshold));
            } catch (IOException e) {
              resultFuture.setException(e);
            }
          }
        });
        return resultFuture;
      }
    }), new Function<StreamProperty, Integer>() {
      @Override
      public Integer apply(StreamProperty property) {
        return property.getThreshold();
      }
    });
  }

  @Override
  public Cancellable addListener(String streamName, StreamPropertyListener listener) {
    return propertyStore.get().addChangeListener(streamName,
                                                 new StreamPropertyChangeListener(streamAdmin, streamName, listener));
  }

  @Override
  protected final void shutDown() throws Exception {
    propertyStore.get().close();
    doShutDown();
  }

  /**
   * Stop the service.
   *
   * @throws Exception when stopping the service could not be performed
   */
  protected abstract void doShutDown() throws Exception;

  /**
   * Object for holding property value in the property store.
   */
  private static final class StreamProperty {

    /**
     * Generation of the stream. {@code null} to ignore this field.
     */
    private final int generation;
    /**
     * TTL of the stream. {@code null} to ignore this field.
     */
    private final long ttl;

    /**
     * Notification threshold of the stream.
     */
    private final int threshold;

    private StreamProperty(int generation, long ttl, int threshold) {
      this.generation = generation;
      this.ttl = ttl;
      this.threshold = threshold;
    }

    public int getGeneration() {
      return generation;
    }

    public long getTTL() {
      return ttl;
    }

    public int getThreshold() {
      return threshold;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("generation", generation)
        .add("ttl", ttl)
        .add("threshold", threshold)
        .toString();
    }
  }

  /**
   * Codec for {@link StreamProperty}.
   */
  private static final class StreamPropertyCodec implements Codec<StreamProperty> {

    private static final Gson GSON = new Gson();

    @Override
    public byte[] encode(StreamProperty property) throws IOException {
      return GSON.toJson(property).getBytes(Charsets.UTF_8);
    }

    @Override
    public StreamProperty decode(byte[] data) throws IOException {
      return GSON.fromJson(new String(data, Charsets.UTF_8), StreamProperty.class);
    }
  }

  /**
   * A {@link PropertyChangeListener} that convert onChange callback into {@link StreamPropertyListener}.
   */
  private final class StreamPropertyChangeListener extends StreamPropertyListener
                                                   implements PropertyChangeListener<StreamProperty> {

    private final StreamPropertyListener listener;
    // Callback from PropertyStore is
    private StreamProperty currentProperty;

    private StreamPropertyChangeListener(StreamAdmin streamAdmin, String streamName, StreamPropertyListener listener) {
      this.listener = listener;
      try {
        StreamConfig streamConfig = streamAdmin.getConfig(streamName);
        this.currentProperty = new StreamProperty(StreamUtils.getGeneration(streamConfig), streamConfig.getTTL(),
                                                  streamConfig.getNotificationThresholdMB());
      } catch (Exception e) {
        // It's ok if the stream config is not yet available (meaning no data has ever been writen to the stream yet.
        this.currentProperty = new StreamProperty(0, Long.MAX_VALUE,
                                                  cConf.getInt(Constants.Stream.NOTIFICATION_THRESHOLD));
      }
    }

    @Override
    public void onChange(String name, StreamProperty newProperty) {
      try {
        if (newProperty != null) {
          if (currentProperty == null || currentProperty.getGeneration() < newProperty.getGeneration()) {
            generationChanged(name, newProperty.getGeneration());
          }

          if (currentProperty == null || currentProperty.getTTL() != newProperty.getTTL()) {
            ttlChanged(name, newProperty.getTTL());
          }

          if (currentProperty == null || currentProperty.getThreshold() != newProperty.getThreshold()) {
            thresholdChanged(name, newProperty.getThreshold());
          }

        } else {
          generationDeleted(name);
          ttlDeleted(name);
        }
      } finally {
        currentProperty = newProperty;
      }
    }

    @Override
    public void onError(String name, Throwable failureCause) {
      LOG.error("Exception on PropertyChangeListener for stream {}", name, failureCause);
    }

    @Override
    public void generationChanged(String streamName, int generation) {
      try {
        listener.generationChanged(streamName, generation);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationChanged", t);
      }
    }

    @Override
    public void generationDeleted(String streamName) {
      try {
        listener.generationDeleted(streamName);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationDeleted", t);
      }
    }

    @Override
    public void ttlChanged(String streamName, long ttl) {
      try {
        listener.ttlChanged(streamName, ttl);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlChanged", t);
      }
    }

    @Override
    public void ttlDeleted(String streamName) {
      try {
        listener.ttlDeleted(streamName);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlDeleted", t);
      }
    }

    @Override
    public void thresholdChanged(String streamName, int threshold) {
      try {
        listener.thresholdChanged(streamName, threshold);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.thresholdChanged", t);
      }
    }
  }
}
