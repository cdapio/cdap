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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.module.DatasetType;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Wraps implementation of {@link Dataset} into a {@link DatasetModule}.
 *
 * This allows for easier implementation of simple datasets without requiring to implement {@link DatasetDefinition},
 * {@link co.cask.cdap.api.dataset.DatasetAdmin}, etc. when the implementation uses existing dataset types.
 *
 * NOTE: all admin ops of the dataset will be delegated to embedded datasets;
 *       {@link co.cask.cdap.api.dataset.DatasetProperties} will be propagated to embedded datasets as well
 *
 * NOTE: must have exactly one constructor with parameter types of
 *       (DatasetSpecification, [0..n] @EmbeddedDataset Dataset)
 *
 * <p>
 * Example of usage:
 *
 * <pre>
 * {@code

// ...

datasetFramework.addModule("myModule", new SingleTypeModule(SimpleKVTable.class));

// ...


&#64;DatasetType("KVTable")
public class SimpleKVTable extends AbstractDataset implements KeyValueTable {
  private static final byte[] COL = new byte[0];

  private final Table table;

  public SimpleKVTable(DatasetSpecification spec,
                       &#64;EmbeddedDataset("data") Table table) {
    super(spec.getTransactionAwareName(), table);
    this.table = table;
  }

  public void put(String key, String value) throws Exception {
    table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
  }

  public String get(String key) throws Exception {
    byte[] value = table.get(Bytes.toBytes(key), COL);
    return value == null ? null : Bytes.toString(value);
  }
}

 * }
 * </pre>
 *
 * See {@link DatasetType} and {@link EmbeddedDataset} for more details on their usage.
 *
 */
public class SingleTypeModule implements DatasetModule {

  private final Class<? extends Dataset> dataSetClass;

  public SingleTypeModule(Class<? extends Dataset> dataSetClass) {
    this.dataSetClass = dataSetClass;
  }

  public Class<? extends Dataset> getDataSetClass() {
    return dataSetClass;
  }

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    final Constructor ctor = findSuitableCtorOrFail(dataSetClass);

    DatasetType typeAnn = dataSetClass.getAnnotation(DatasetType.class);
    // default type name to dataset class name
    String typeName = typeAnn != null ? typeAnn.value() : dataSetClass.getName();

    // The ordering is important. It is the same order as the parameters
    final Map<String, DatasetDefinition> embeddedDefinitions = Maps.newLinkedHashMap();

    final Class<?>[] paramTypes = ctor.getParameterTypes();
    Annotation[][] paramAnns = ctor.getParameterAnnotations();

    // Gather all dataset name and type information for the @EmbeddedDataset parameters
    for (int i = 1; i < paramTypes.length; i++) {
      // Must have the EmbeddedDataset as it's the contract of the findSuitableCtorOrFail method
      EmbeddedDataset anno = Iterables.filter(Arrays.asList(paramAnns[i]), EmbeddedDataset.class).iterator().next();
      String type = anno.type();
      // default to dataset class name if dataset type name is not specified through the annotation
      if (EmbeddedDataset.DEFAULT_TYPE_NAME.equals(type)) {
        type = paramTypes[i].getName();
      }
      DatasetDefinition embeddedDefinition = registry.get(type);
      if (embeddedDefinition == null) {
        throw new IllegalStateException(
          String.format("Unknown Dataset type '%s', specified by parameter number %d of the %s Dataset",
                        type, i, dataSetClass.getName()));
      }
      embeddedDefinitions.put(anno.value(), embeddedDefinition);
    }

    registry.add(new CompositeDatasetDefinition<Dataset>(typeName, embeddedDefinitions) {
      @Override
      public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                Map<String, String> arguments, ClassLoader classLoader) throws IOException {
        List<Object> params = new ArrayList<>();
        params.add(spec);
        for (Map.Entry<String, DatasetDefinition> entry : embeddedDefinitions.entrySet()) {
          params.add(entry.getValue().getDataset(datasetContext, spec.getSpecification(entry.getKey()),
                                                 arguments, classLoader));
        }
        try {
          return (Dataset) ctor.newInstance(params.toArray());
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  /**
   * Find a {@link Constructor} of the given {@link Dataset} class that the first parameter is of type
   * {@link DatasetSpecification} and the rest parameters are of type {@link Dataset} and are annotated with
   * {@link EmbeddedDataset}.
   *
   * @param dataSetClass The class to search for the constructor
   * @return the {@link Constructor} found
   * @throws IllegalArgumentException if the given class doesn't contain the constructor this method is looking for
   */
  @VisibleForTesting
  static Constructor findSuitableCtorOrFail(Class<? extends Dataset> dataSetClass) {
    Constructor suitableCtor = null;
    for (Constructor ctor : dataSetClass.getConstructors()) {
      Class<?>[] paramTypes = ctor.getParameterTypes();

      // Ignore constructor that has no-arg or the first arg is not DatasetSpecification
      if (paramTypes.length <= 0 || !DatasetSpecification.class.isAssignableFrom(paramTypes[0])) {
        continue;
      }

      Annotation[][] paramAnns = ctor.getParameterAnnotations();
      boolean otherParamsAreDatasets = true;
      // All parameters must be of Dataset type and annotated with EmbeddedDataset
      for (int i = 1; i < paramTypes.length; i++) {
        if (!Dataset.class.isAssignableFrom(paramTypes[i])
            || !Iterables.any(Arrays.asList(paramAnns[i]), Predicates.instanceOf(EmbeddedDataset.class))) {
          otherParamsAreDatasets = false;
          break;
        }
      }
      if (!otherParamsAreDatasets) {
        continue;
      }
      if (suitableCtor != null) {
        throw new IllegalArgumentException(
          String.format("Dataset class %s must have single constructor with parameter types of" +
                          " (DatasetSpecification, [0..n] @EmbeddedDataset Dataset) ", dataSetClass));
      }
      suitableCtor = ctor;
    }

    if (suitableCtor == null) {
      throw new IllegalArgumentException(
        String.format("Dataset class %s must have single constructor with parameter types of" +
                        " (DatasetSpecification, [0..n] @EmbeddedDataset Dataset) ", dataSetClass));
    }

    return suitableCtor;
  }
}
