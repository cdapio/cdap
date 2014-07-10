package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.CompositeDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.module.DatasetType;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Wraps implementation of {@link Dataset} into a {@link DatasetModule}.
 *
 * This allows for easier implementation of simple datasets without requiring to implement {@link DatasetDefinition},
 * {@link com.continuuity.api.dataset.DatasetAdmin}, etc. when the implementation uses existing dataset types.
 *
 * NOTE: all admin ops of the dataset will be delegated to embedded datasets;
 *       {@link com.continuuity.api.dataset.DatasetProperties} will be propagated to embedded datasets as well
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
    super(spec.getName(), table);
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
  private static final Logger LOG = LoggerFactory.getLogger(SingleTypeModule.class);

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

    final Map<String, DatasetDefinition> defs = Maps.newHashMap();

    Class<?>[] paramTypes = ctor.getParameterTypes();
    Annotation[][] paramAnns = ctor.getParameterAnnotations();

    // computing parameters for dataset constructor:
    // if param is of type DatasetSpecification we'll need to set spec as a value
    // if param has EmbeddedDataset annotation we need to set instance of embedded dataset as a value
    final DatasetCtorParam[] ctorParams = new DatasetCtorParam[paramTypes.length];
    for (int i = 0; i < paramTypes.length; i++) {
      if (DatasetSpecification.class.isAssignableFrom(paramTypes[i])) {
        ctorParams[i] = new DatasetSpecificationParam();
        continue;
      }
      for (Annotation ann : paramAnns[i]) {
        if (ann instanceof EmbeddedDataset) {
          String type = ((EmbeddedDataset) ann).type();
          if (EmbeddedDataset.DEFAULT_TYPE_NAME.equals(type)) {
            // default to dataset class name
            type = paramTypes[i].getName();
          }
          DatasetDefinition def = registry.get(type);
          if (def == null) {
            String msg = String.format("Unknown data set type used with @Dataset: " + type);
            LOG.error(msg);
            throw new IllegalStateException(msg);
          }
          defs.put(((EmbeddedDataset) ann).value(), def);
          ctorParams[i] = new DatasetParam(((EmbeddedDataset) ann).value());
          break;
        }
      }
    }

    CompositeDatasetDefinition def = new CompositeDatasetDefinition(typeName, defs) {
      @Override
      public Dataset getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
        Object[] params = new Object[ctorParams.length];
        for (int i = 0; i < ctorParams.length; i++) {
          params[i] = ctorParams[i] != null ? ctorParams[i].getValue(defs, spec, classLoader) : null;
        }

        try {
          return (Dataset) ctor.newInstance(params);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    registry.add(def);
  }

  @VisibleForTesting
  static Constructor findSuitableCtorOrFail(Class<? extends Dataset> dataSetClass) {
    Constructor[] ctors = dataSetClass.getConstructors();
    Constructor suitableCtor = null;
    for (Constructor ctor : ctors) {
      Class<?>[] paramTypes = ctor.getParameterTypes();
      Annotation[][] paramAnns = ctor.getParameterAnnotations();
      boolean firstParamIsSpec = paramTypes.length > 0 &&
        DatasetSpecification.class.isAssignableFrom(paramTypes[0]);

      if (firstParamIsSpec) {
        boolean otherParamsAreDatasets = true;
        // checking type of the param
        for (int i = 1; i < paramTypes.length; i++) {
          if (!Dataset.class.isAssignableFrom(paramTypes[i])) {
            otherParamsAreDatasets = false;
            break;
          }
          // checking that annotation is there
          boolean hasAnnotation = false;
          for (Annotation ann : paramAnns[i]) {
            if (ann instanceof EmbeddedDataset) {
              hasAnnotation = true;
              break;
            }
          }
          if (!hasAnnotation) {
            otherParamsAreDatasets = false;
          }
        }
        if (otherParamsAreDatasets) {
          if (suitableCtor != null) {
            throw new IllegalArgumentException(
              String.format("Dataset class %s must have single constructor with parameter types of" +
                              " (DatasetSpecification, [0..n] @EmbeddedDataset Dataset) ", dataSetClass));
          }

          suitableCtor = ctor;
        }
      }
    }

    if (suitableCtor == null) {
      throw new IllegalArgumentException(
        String.format("Dataset class %s must have single constructor with parameter types of" +
                        " (DatasetSpecification, [0..n] @EmbeddedDataset Dataset) ", dataSetClass));
    }

    return suitableCtor;
  }

  private interface DatasetCtorParam {
    Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec, ClassLoader cl)
      throws IOException;
  }

  private static final class DatasetSpecificationParam implements DatasetCtorParam {
    @Override
    public Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec, ClassLoader cl) {
      return spec;
    }
  }

  private static final class DatasetParam implements DatasetCtorParam {
    private final String name;

    private DatasetParam(String name) {
      this.name = name;
    }

    @Override
    public Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec, ClassLoader cl)
      throws IOException {

      return defs.get(name).getDataset(spec.getSpecification(name), cl);
    }
  }
}
