package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.Schema;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * @param <T>
 */
public final class DatumOutputEmitter<T> implements OutputEmitter<T> {

  public static final Function<Object, Integer> PARTITION_MAP_TRANSFORMER = new PartitionMapTransformer();

  private final Queue2Producer queueProducer;
  private final byte[] schemaHash;
  private final DatumWriter<T> writer;

  public DatumOutputEmitter(Queue2Producer queueProducer, Schema schema, DatumWriter<T> writer) {
    this.queueProducer = queueProducer;
    this.schemaHash = schema.getSchemaHash().toByteArray();
    this.writer = writer;
  }

  @Override
  public void emit(T data) {
    emit(data, ImmutableMap.<String, Object>of());
  }

  @Override
  public void emit(T data, String partitionKey, Object partitionValue) {
    emit(data, ImmutableMap.of(partitionKey, partitionValue));
  }

  @Override
  public void emit(T data, Map<String, Object> partitions) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      output.write(schemaHash);
      writer.encode(data, new BinaryEncoder(output));
      queueProducer.enqueue(new QueueEntry(Maps.transformValues(partitions, PARTITION_MAP_TRANSFORMER),
                                           output.toByteArray()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static final class PartitionMapTransformer implements Function<Object, Integer> {
    @Override
    public Integer apply(@Nullable Object input) {
      return input == null ? 0 : input.hashCode();
    }
  }
}
