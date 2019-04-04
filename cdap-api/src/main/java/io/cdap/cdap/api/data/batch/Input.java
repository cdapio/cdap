/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.data.batch;

import io.cdap.cdap.api.common.RuntimeArguments;

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
  private String namespace;

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
   * Sets the namespace of the input.
   *
   * @param namespace the namespace of the input
   * @return the Input being operated on
   */
  public Input fromNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * @return The namespace of the input.
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return an alias of the input, to be used as the input name instead of the actual name of the input (i.e. dataset
   * name). Defaults to the actual name, in the case that no alias was set.
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
   *  @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   */
  public static Input ofDataset(String datasetName, Map<String, String> arguments) {
    return ofDataset(datasetName, arguments, null);
  }

  /**
   * Returns an Input defined by a dataset.
   *  @param datasetName the name of the input dataset
   * @param splits the data selection splits. If null, will use the splits defined by the dataset. If the dataset
   *               type is not {@link BatchReadable}, splits will be ignored
   */
  public static Input ofDataset(String datasetName, @Nullable Iterable<? extends Split> splits) {
    return ofDataset(datasetName, RuntimeArguments.NO_ARGUMENTS, splits);
  }

  /**
   * Returns an Input defined by a dataset.
   *  @param datasetName the name of the input dataset
   * @param arguments the arguments to use when instantiating the dataset
   * @param splits the data selection splits. If null, will use the splits defined by the dataset. If the dataset
   *               type is not {@link BatchReadable}, splits will be ignored
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
   * An implementation of {@link Input}, which defines a {@link io.cdap.cdap.api.dataset.Dataset} as an input.
   */
  public static class DatasetInput extends Input {
    private final Map<String, String> arguments;
    private final List<Split> splits;

    private DatasetInput(String name, Map<String, String> arguments, @Nullable Iterable<? extends Split> splits) {
      super(name);
      this.arguments = Collections.unmodifiableMap(new HashMap<>(arguments));
      this.splits = copySplits(splits);
    }

    private DatasetInput(String name, Map<String, String> arguments, @Nullable Iterable<? extends Split> splits,
                         String namespace) {
      this(name, arguments, splits);
      super.fromNamespace(namespace);
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

    @Override
    public DatasetInput fromNamespace(String namespace) {
      return new DatasetInput(super.name, arguments, splits, namespace);
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

    @Override
    public Input fromNamespace(String namespace) {
      throw new UnsupportedOperationException("InputFormatProviderInput does not support setting namespace.");
    }
  }
}
