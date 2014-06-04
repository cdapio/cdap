/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.async.ExecutorUtils;
import com.continuuity.common.conf.PropertyChangeListener;
import com.continuuity.common.conf.PropertyStore;
import com.continuuity.common.conf.PropertyUpdater;
import com.continuuity.common.io.Codec;
import com.continuuity.common.io.Locations;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Base implementation for {@link StreamCoordinator}.
 */
public abstract class AbstractStreamCoordinator implements StreamCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamCoordinator.class);

  // Executor for performing update action asynchronously
  private final Executor updateExecutor;
  private final Supplier<PropertyStore<StreamProperty>> propertyStore;

  protected AbstractStreamCoordinator() {
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
              int newGeneration = ((property == null) ? lowerBound : property.getGeneration()) + 1;
              // Create the generation directory
              Locations.mkdirsIfNotExists(StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                               newGeneration));
              resultFuture.set(new StreamProperty(newGeneration));
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
  public Cancellable addListener(String streamName, StreamPropertyListener listener) {
    return propertyStore.get().addChangeListener(streamName, new StreamPropertyChangeListener(listener));
  }

  @Override
  public void close() throws IOException {
    propertyStore.get().close();
  }

  /**
   * Object for holding property value in the property store.
   */
  private static final class StreamProperty {

    private final int generation;

    private StreamProperty(int generation) {
      this.generation = generation;
    }

    public int getGeneration() {
      return generation;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("generation", generation)
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
  private static final class StreamPropertyChangeListener extends StreamPropertyListener
                                                          implements PropertyChangeListener<StreamProperty> {

    private final StreamPropertyListener listener;
    // Callback from PropertyStore is
    private StreamProperty currentProperty;

    private StreamPropertyChangeListener(StreamPropertyListener listener) {
      this.listener = listener;
    }

    @Override
    public void onChange(String name, StreamProperty property) {
      try {
        if (property == null) {
          // Property is deleted
          if (currentProperty != null) {
            // Fire all delete events
            generationDeleted(name);
          }
          return;
        }

        if (currentProperty == null) {
          // Fire all events
          generationChanged(name, property.getGeneration());
          return;
        }

        // Inspect individual stream property to determine what needs to be fired
        if (currentProperty.getGeneration() < property.getGeneration()) {
          generationChanged(name, property.getGeneration());
        }
      } finally {
        currentProperty = property;
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
        LOG.error("Exception while calling StreamPropertyListener", t);
      }
    }

    @Override
    public void generationDeleted(String streamName) {
      try {
        listener.generationDeleted(streamName);
      } catch (Throwable t) {
        LOG.error("Exception while calling StreamPropertyListener", t);
      }
    }
  }
}
