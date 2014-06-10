package com.continuuity.data2.dataset2;

import com.continuuity.data2.dataset2.lib.CompositeDatasetDefinition;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.continuuity.internal.data.dataset.module.DatasetType;
import com.continuuity.internal.data.dataset.module.EmbeddedDataset;
import com.google.common.base.Preconditions;
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
 * {@link com.continuuity.internal.data.dataset.DatasetAdmin}, etc. when the implementation uses existing dataset types.
 *
 * NOTE: all admin ops of the dataset will be delegated to embedded datasets;
 *       {@link com.continuuity.internal.data.dataset.DatasetProperties} will be propagated to embedded datasets as well
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

  private final Class<? extends com.continuuity.internal.data.dataset.Dataset> dataSetClass;

  public SingleTypeModule(Class<? extends com.continuuity.internal.data.dataset.Dataset> dataSetClass) {
    this.dataSetClass = dataSetClass;
  }

  public Class<? extends Dataset> getDataSetClass() {
    return dataSetClass;
  }

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    Constructor[] ctors = dataSetClass.getConstructors();
    Preconditions.checkArgument(ctors.length == 1, "Dataset class %s must have one constructor", dataSetClass);
    final Constructor ctor = ctors[0];

    DatasetType typeAnn = dataSetClass.getAnnotation(DatasetType.class);
    // default type name to dataset class name
    String typeName = typeAnn != null ? typeAnn.value() : dataSetClass.getName();

    final Map<String, DatasetDefinition> defs = Maps.newHashMap();

    Class<?>[] paramTypes = ctor.getParameterTypes();
    Annotation[][] paramAnns = ctor.getParameterAnnotations();

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
      public com.continuuity.internal.data.dataset.Dataset getDataset(DatasetSpecification spec) throws IOException {
        Object[] params = new Object[ctorParams.length];
        for (int i = 0; i < ctorParams.length; i++) {
          params[i] = ctorParams[i] != null ? ctorParams[i].getValue(defs, spec) : null;
        }

        try {
          return (com.continuuity.internal.data.dataset.Dataset) ctor.newInstance(params);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    registry.add(def);
  }

  private static interface DatasetCtorParam {
    Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec) throws IOException;
  }

  private static final class DatasetSpecificationParam implements DatasetCtorParam {
    @Override
    public Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec) {
      return spec;
    }
  }

  private static final class DatasetParam implements DatasetCtorParam {
    private final String name;

    private DatasetParam(String name) {
      this.name = name;
    }

    @Override
    public Object getValue(Map<String, DatasetDefinition> defs, DatasetSpecification spec) throws IOException {
      return defs.get(name).getDataset(spec.getSpecification(name));
    }
  }
}
