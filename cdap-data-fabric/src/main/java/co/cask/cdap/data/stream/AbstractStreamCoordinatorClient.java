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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.conf.PropertyChangeListener;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.conf.SyncPropertyUpdater;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;

/**
 * Base implementation for {@link StreamCoordinatorClient}.
 */
public abstract class AbstractStreamCoordinatorClient extends AbstractIdleService implements StreamCoordinatorClient {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCoordinatorClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private PropertyStore<CoordinatorStreamProperties> propertyStore;

  /**
   * Starts the service.
   *
   * @throws Exception when starting of the service failed
   */
  protected abstract void doStartUp() throws Exception;

  /**
   * Stops the service.
   *
   * @throws Exception when stopping the service could not be performed
   */
  protected abstract void doShutDown() throws Exception;

  /**
   * Creates a {@link PropertyStore}.
   *
   * @param codec Codec for the property stored in the property store
   * @param <T> Type of the property
   * @return A new {@link PropertyStore}.
   */
  protected abstract <T> PropertyStore<T> createPropertyStore(Codec<T> codec);

  /**
   * Returns a {@link Lock} for performing exclusive operation for the given stream.
   */
  protected abstract Lock getLock(Id.Stream streamId);

  /**
   * Gets invoked when a stream of the given name is created.
   */
  protected abstract void streamCreated(Id.Stream streamId);

  @Override
  public StreamConfig createStream(Id.Stream streamId, Callable<StreamConfig> action) throws Exception {
    Lock lock = getLock(streamId);
    lock.lock();
    try {
      StreamConfig config = action.call();
      if (config != null) {
        streamCreated(streamId);
      }
      return config;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void updateProperties(Id.Stream streamId, Callable<CoordinatorStreamProperties> action) throws Exception {
    Lock lock = getLock(streamId);
    lock.lock();
    try {
      final CoordinatorStreamProperties properties = action.call();
      propertyStore.update(streamId.toId(), new SyncPropertyUpdater<CoordinatorStreamProperties>() {

        @Override
        protected CoordinatorStreamProperties compute(@Nullable CoordinatorStreamProperties oldProperties) {
          if (oldProperties == null) {
            return properties;
          }
          // Merge the old and new properties.
          return new CoordinatorStreamProperties(
            properties.getName(),
            firstNotNull(properties.getTTL(), oldProperties.getTTL()),
            firstNotNull(properties.getFormat(), oldProperties.getFormat()),
            firstNotNull(properties.getThreshold(), oldProperties.getThreshold()),
            firstNotNull(properties.getGeneration(), oldProperties.getGeneration()));
        }
      }).get();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Cancellable addListener(Id.Stream streamId, StreamPropertyListener listener) {
    return propertyStore.addChangeListener(streamId.toId(), new StreamPropertyChangeListener(listener));
  }

  @Override
  protected final void startUp() throws Exception {
    propertyStore = createPropertyStore(new Codec<CoordinatorStreamProperties>() {
      @Override
      public byte[] encode(CoordinatorStreamProperties properties) throws IOException {
        return Bytes.toBytes(GSON.toJson(properties));
      }

      @Override
      public CoordinatorStreamProperties decode(byte[] data) throws IOException {
        return GSON.fromJson(Bytes.toString(data), CoordinatorStreamProperties.class);
      }
    });

    try {
      doStartUp();
    } catch (Exception e) {
      propertyStore.close();
      throw e;
    }
  }

  @Override
  protected final void shutDown() throws Exception {
    propertyStore.close();
    doShutDown();
  }

  /**
   * Returns first if first is not {@code null}, otherwise return second.
   * It is different than Guava {@link Objects#firstNonNull(Object, Object)} in the way that it allows the second
   * parameter to be null.
   */
  @Nullable
  private <T> T firstNotNull(@Nullable T first, @Nullable T second) {
    return first != null ? first : second;
  }

  /**
   * A {@link PropertyChangeListener} that convert onChange callback into {@link StreamPropertyListener}.
   */
  private final class StreamPropertyChangeListener extends StreamPropertyListener
    implements PropertyChangeListener<CoordinatorStreamProperties> {

    private final StreamPropertyListener listener;
    private CoordinatorStreamProperties oldProperties;

    private StreamPropertyChangeListener(StreamPropertyListener listener) {
      this.listener = listener;
    }

    @Override
    public void onChange(String name, CoordinatorStreamProperties properties) {
      Id.Stream streamId = Id.Stream.fromId(name);
      if (properties == null) {
        generationDeleted(streamId);
        ttlDeleted(streamId);
        oldProperties = null;
        return;
      }

      Integer generation = properties.getGeneration();
      Integer oldGeneration = (oldProperties == null) ? null : oldProperties.getGeneration();
      if (generation != null && (oldGeneration == null || generation > oldGeneration)) {
        generationChanged(streamId, generation);
      }

      Long ttl = properties.getTTL();
      Long oldTTL = (oldProperties == null) ? null : oldProperties.getTTL();
      if (ttl != null && !ttl.equals(oldTTL)) {
        ttlChanged(streamId, ttl);
      }

      Integer threshold = properties.getThreshold();
      Integer oldThreshold = (oldProperties == null) ? null : oldProperties.getThreshold();

      if (threshold != null && !threshold.equals(oldThreshold)) {
        thresholdChanged(streamId, threshold);
      }
      oldProperties = properties;
    }

    @Override
    public void onError(String name, Throwable failureCause) {
      LOG.error("Exception on PropertyChangeListener for stream {}", name, failureCause);
    }

    @Override
    public void generationChanged(Id.Stream streamId, int generation) {
      try {
        listener.generationChanged(streamId, generation);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationChanged", t);
      }
    }

    @Override
    public void generationDeleted(Id.Stream streamId) {
      try {
        listener.generationDeleted(streamId);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.generationDeleted", t);
      }
    }

    @Override
    public void ttlChanged(Id.Stream streamId, long ttl) {
      try {
        listener.ttlChanged(streamId, ttl);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlChanged", t);
      }
    }

    @Override
    public void ttlDeleted(Id.Stream streamId) {
      try {
        listener.ttlDeleted(streamId);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.ttlDeleted", t);
      }
    }

    @Override
    public void thresholdChanged(Id.Stream streamId, int threshold) {
      try {
        listener.thresholdChanged(streamId, threshold);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener.thresholdChanged", t);
      }
    }
  }
}
