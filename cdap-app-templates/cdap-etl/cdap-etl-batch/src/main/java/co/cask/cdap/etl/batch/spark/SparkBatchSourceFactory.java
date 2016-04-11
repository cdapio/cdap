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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import static java.lang.Thread.currentThread;

/**
 * A POJO class for storing source information being set from {@link SparkBatchSourceContext} and used in
 * {@link ETLSparkProgram}.
 */
final class SparkBatchSourceFactory {

  private enum SourceType {
    STREAM(1), PROVIDER(2), DATASET(3);

    private final byte id;

    SourceType(int id) {
      this.id = (byte) id;
    }

    static SourceType from(byte id) {
      for (SourceType type : values()) {
        if (type.id == id) {
          return type;
        }
      }
      throw new IllegalArgumentException("No SourceType found for id " + id);
    }
  }

  private final StreamBatchReadable streamBatchReadable;
  private final InputFormatProvider inputFormatProvider;
  private final DatasetInfo datasetInfo;

  static SparkBatchSourceFactory create(StreamBatchReadable streamBatchReadable) {
    return new SparkBatchSourceFactory(streamBatchReadable, null, null);
  }

  static SparkBatchSourceFactory create(InputFormatProvider inputFormatProvider) {
    return new SparkBatchSourceFactory(null, inputFormatProvider, null);
  }

  static SparkBatchSourceFactory create(String datasetName) {
    return create(datasetName, ImmutableMap.<String, String>of());
  }

  static SparkBatchSourceFactory create(String datasetName, Map<String, String> datasetArgs) {
    return create(datasetName, datasetArgs, null);
  }

  static SparkBatchSourceFactory create(String datasetName, Map<String, String> datasetArgs,
                                        @Nullable List<Split> splits) {
    return new SparkBatchSourceFactory(null, null, new DatasetInfo(datasetName, datasetArgs, splits));
  }

  static SparkBatchSourceFactory deserialize(InputStream inputStream) throws IOException {
    DataInput input = new DataInputStream(inputStream);

    // Deserialize based on the type
    switch (SourceType.from(input.readByte())) {
      case STREAM:
        return new SparkBatchSourceFactory(new StreamBatchReadable(URI.create(input.readUTF())), null, null);
      case PROVIDER:
        return new SparkBatchSourceFactory(
          null, new BasicInputFormatProvider(
          input.readUTF(), Serializations.deserializeMap(input, Serializations.createStringObjectReader())), null
        );
      case DATASET:
        return new SparkBatchSourceFactory(null, null, DatasetInfo.deserialize(input));
    }
    throw new IllegalArgumentException("Invalid input. Failed to decode SparkBatchSourceFactory.");
  }

  private SparkBatchSourceFactory(@Nullable StreamBatchReadable streamBatchReadable,
                                  @Nullable InputFormatProvider inputFormatProvider,
                                  @Nullable DatasetInfo datasetInfo) {
    this.streamBatchReadable = streamBatchReadable;
    this.inputFormatProvider = inputFormatProvider;
    this.datasetInfo = datasetInfo;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    DataOutput output = new DataOutputStream(outputStream);
    if (streamBatchReadable != null) {
      output.writeByte(SourceType.STREAM.id);
      output.writeUTF(streamBatchReadable.toURI().toString());
      return;
    }
    if (inputFormatProvider != null) {
      output.writeByte(SourceType.PROVIDER.id);
      output.writeUTF(inputFormatProvider.getInputFormatClassName());
      Serializations.serializeMap(inputFormatProvider.getInputFormatConfiguration(),
                                  Serializations.createStringObjectWriter(), output);
      return;
    }
    if (datasetInfo != null) {
      output.writeByte(SourceType.DATASET.id);
      datasetInfo.serialize(output);
      return;
    }
    // This should never happen since the constructor is private and it only get calls from static create() methods
    // which make sure one and only one of those source type will be specified.
    throw new IllegalStateException("Unknown source type");
  }

  public <K, V> JavaPairRDD<K, V> createRDD(JavaSparkExecutionContext sec, JavaSparkContext jsc,
                                            Class<K> keyClass, Class<V> valueClass) {
    if (streamBatchReadable != null) {
      String decoderType = streamBatchReadable.getDecoderType();
      if (decoderType == null) {
        return (JavaPairRDD<K, V>) sec.fromStream(streamBatchReadable.getStreamName(),
                                                  streamBatchReadable.getStartTime(),
                                                  streamBatchReadable.getEndTime(),
                                                  valueClass);
      } else {
        try {
          @SuppressWarnings("unchecked")
          Class<StreamEventDecoder<K, V>> decoderClass =
            (Class<StreamEventDecoder<K, V>>) Thread.currentThread().getContextClassLoader().loadClass(decoderType);
          return sec.fromStream(streamBatchReadable.getStreamName(),
                                streamBatchReadable.getStartTime(),
                                streamBatchReadable.getEndTime(),
                                decoderClass, keyClass, valueClass);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }
    if (inputFormatProvider != null) {
      Configuration hConf = new Configuration();
      hConf.clear();
      for (Map.Entry<String, String> entry : inputFormatProvider.getInputFormatConfiguration().entrySet()) {
        hConf.set(entry.getKey(), entry.getValue());
      }
      ClassLoader classLoader = Objects.firstNonNull(currentThread().getContextClassLoader(),
                                                     getClass().getClassLoader());
      try {
        @SuppressWarnings("unchecked")
        Class<InputFormat> inputFormatClass = (Class<InputFormat>) classLoader.loadClass(
          inputFormatProvider.getInputFormatClassName());
        return jsc.newAPIHadoopRDD(hConf, inputFormatClass, keyClass, valueClass);
      } catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }
    }
    if (datasetInfo != null) {
      return sec.fromDataset(datasetInfo.getDatasetName(), datasetInfo.getDatasetArgs());
    }
    // This should never happen since the constructor is private and it only get calls from static create() methods
    // which make sure one and only one of those source type will be specified.
    throw new IllegalStateException("Unknown source type");
  }

  private static final class BasicInputFormatProvider implements InputFormatProvider {

    private final String inputFormatClassName;
    private final Map<String, String> configuration;

    private BasicInputFormatProvider(String inputFormatClassName, Map<String, String> configuration) {
      this.inputFormatClassName = inputFormatClassName;
      this.configuration = ImmutableMap.copyOf(configuration);
    }

    @Override
    public String getInputFormatClassName() {
      return inputFormatClassName;
    }

    @Override
    public Map<String, String> getInputFormatConfiguration() {
      return configuration;
    }
  }
}
