/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.data.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines input to a program, such as MapReduce.
 */
public abstract class Input {

  private final String name;

  private String alias;

  private Input(String name) {
    this.name = name;
  }

  /**
   * @return The name of the input.
   */
  public String getName() {
    return name;
  }

  /**
   * @return an alias of the input, to be used as the input name instead of the actual name of the input (i.e. dataset
   * name or stream name). Defaults to the actual name, in the case that no alias was set.
   */
  public String getAlias() {
    return alias == null ? name : alias;
  }

  /**
   * Sets an alias to be used as the input name.
   *
   * @param alias the alias to be set for this Input
   * @return the Input being operated on
   */
  public Input alias(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Returns an Input defined by a dataset.
   *
   * @param datasetName the name of the input dataset
   */
  public static Input ofDataset(String datasetName) {
    return ofDataset(datasetName, RuntimeArguments.NO_ARGUMENTS);
  }

  /**
   * Returns an Input defined by a dataset.
   *
   * @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   */
  public static Input ofDataset(String datasetName, Map<String, String> arguments) {
    return ofDataset(datasetName, arguments, null);
  }

  /**
   * Returns an Input defined by a dataset.
   *
   * @param datasetName the name of the input dataset
   * @param splits the data selection splits. If null, will use the splits defined by the dataset.
   */
  public static Input ofDataset(String datasetName, @Nullable Iterable<? extends Split> splits) {
    return ofDataset(datasetName, RuntimeArguments.NO_ARGUMENTS, splits);
  }

  /**
   * Returns an Input defined by a dataset.
   *
   * @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @param splits the data selection splits. If null, will use the splits defined by the dataset.
   */
  public static Input ofDataset(String datasetName, Map<String, String> arguments,
                                @Nullable Iterable<? extends Split> splits) {
    return new DatasetInput(datasetName, arguments, splits);
  }

  /**
   * Returns an Input defined by an InputFormatProvider.
   *
   * @param inputName the name of the input
   */
  public static Input of(String inputName, InputFormatProvider inputFormatProvider) {
    return new InputFormatProviderInput(inputName, inputFormatProvider);
  }

  /**
   * Returns an Input defined with the given stream name with all time range.
   *
   * @param streamName Name of the stream.
   */
  public static Input ofStream(String streamName) {
    return ofStream(new StreamBatchReadable(streamName, 0, Long.MAX_VALUE));
  }

  /**
   * Returns an Input defined by a stream with the given properties.
   *
   * @param streamName Name of the stream.
   * @param startTime Start timestamp in milliseconds.
   * @param endTime End timestamp in milliseconds.
   */
  public static Input ofStream(String streamName, long startTime, long endTime) {
    return ofStream(new StreamBatchReadable(streamName, startTime, endTime));
  }

  /**
   * Returns an Input defined by a stream with the given properties.
   *
   * @param streamName Name of the stream
   * @param startTime Start timestamp in milliseconds (inclusive) of stream events provided to the job
   * @param endTime End timestamp in milliseconds (exclusive) of stream events provided to the job
   * @param decoderType The {@link StreamEventDecoder} class for decoding {@link StreamEvent}
   */
  public static Input ofStream(String streamName, long startTime,
                               long endTime, Class<? extends StreamEventDecoder> decoderType) {
    return ofStream(new StreamBatchReadable(streamName, startTime, endTime, decoderType));
  }

  /**
   * Returns an Input defined by a stream with the given properties.
   *
   * @param streamName Name of the stream
   * @param startTime Start timestamp in milliseconds (inclusive) of stream events provided to the job
   * @param endTime End timestamp in milliseconds (exclusive) of stream events provided to the job
   * @param bodyFormatSpec The {@link FormatSpecification} class for decoding {@link StreamEvent}
   */
  @Beta
  public static Input ofStream(String streamName, long startTime,
                               long endTime, FormatSpecification bodyFormatSpec) {
    return ofStream(new StreamBatchReadable(streamName, startTime, endTime, bodyFormatSpec));
  }

  /**
   * Returns an Input defined by a stream.
   *
   * @param streamBatchReadable specifies the stream to be used as input
   */
  private static Input ofStream(StreamBatchReadable streamBatchReadable) {
    return new StreamInput(streamBatchReadable);
  }

  /**
   * An implementation of {@link Input}, which defines a {@link co.cask.cdap.api.dataset.Dataset} as an input.
   */
  public static class DatasetInput extends Input {

    private final Map<String, String> arguments;
    private final List<Split> splits;

    private DatasetInput(String name, Map<String, String> arguments, @Nullable Iterable<? extends Split> splits) {
      super(name);
      this.arguments = Collections.unmodifiableMap(new HashMap<>(arguments));
      this.splits = copySplits(splits);
    }

    private List<Split> copySplits(@Nullable Iterable<? extends Split> splitsToCopy) {
      if (splitsToCopy == null) {
        return null;
      }
      List<Split> copiedSplits = new ArrayList<>();
      for (Split split : splitsToCopy) {
        copiedSplits.add(split);
      }
      return copiedSplits;
    }

    public Map<String, String> getArguments() {
      return arguments;
    }

    @Nullable
    public List<Split> getSplits() {
      return splits;
    }
  }

  /**
   * An implementation of {@link Input}, which defines a {@link co.cask.cdap.api.data.stream.Stream} as an input.
   */
  public static class StreamInput extends Input {

    private final StreamBatchReadable streamBatchReadable;

    private StreamInput(StreamBatchReadable streamBatchReadable) {
      super(streamBatchReadable.getStreamName());
      this.streamBatchReadable = streamBatchReadable;
    }

    @Deprecated
    public StreamBatchReadable getStreamBatchReadable() {
      return streamBatchReadable;
    }
  }

  /**
   * An implementation of {@link Input}, which defines an {@link InputFormatProvider} as an input.
   */
  public static class InputFormatProviderInput extends Input {

    private final InputFormatProvider inputFormatProvider;

    private InputFormatProviderInput(String name, InputFormatProvider inputFormatProvider) {
      super(name);
      this.inputFormatProvider = inputFormatProvider;
    }

    public InputFormatProvider getInputFormatProvider() {
      return inputFormatProvider;
    }
  }
}
