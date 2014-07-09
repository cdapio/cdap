/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.FileDataSet;
import com.continuuity.api.data.dataset.MultiObjectStore;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.MemoryTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.metrics.MeteredDataset;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.DataFabric;
import com.continuuity.data.table.RuntimeMemoryTable;
import com.continuuity.data.table.RuntimeTable;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.internal.lang.Fields;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implements the core logic of instantiating a dataset, including injection of the data fabric runtime and
 * built-in datasets.
 */
public class DataSetInstantiationBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataSetInstantiationBase.class);

  private final CConfiguration configuration;
  // the class loader to use for data set classes
  private final ClassLoader classLoader;
  // the known data set specifications
  private final Map<String, DataSetSpecification> datasets = Maps.newHashMap();
  private final Map<String, DatasetCreationSpec> datasetsV2 = Maps.newHashMap();

  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();
  // in this collection we have only datasets initialized with getDataSet() which is OK for now...
  private final Map<TransactionAware, String> txAwareToMetricNames = Maps.newIdentityHashMap();

  private final InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

  public DataSetInstantiationBase(CConfiguration configuration) {
    this(configuration, null);
  }

  public DataSetInstantiationBase(CConfiguration configuration, ClassLoader classLoader) {
    this.configuration = configuration;
    this.classLoader = classLoader;
  }

  /**
   * Set the data set spec for all data sets that this instantiator can
   * create. This should be a list of DataSetSpecification's obtained from actual
   * data sets' configure() method.
   * @param specs The list of DataSetSpecification's
   */
  public void setDataSets(Iterable<DataSetSpecification> specs,
                          Iterable<DatasetCreationSpec> creationSpec) {
    for (DataSetSpecification spec : specs) {
      this.datasets.put(spec.getName(), spec);
    }
    for (DatasetCreationSpec spec : creationSpec) {
      if (spec != null) {
        this.datasetsV2.put(spec.getInstanceName(), spec);
      }
    }
  }

  /**
   * Add one data set spec to this instantiator.
   * @param spec the data set specification
   */
  public void addDataSet(DataSetSpecification spec) {
    this.datasets.put(spec.getName(), spec);
  }

  /**
   * Find out whether the instantiator has a spec for a named data set.
   * @param name the name of the data set
   * @return whether the instantiator knows the spec for the data set
   */
  public boolean hasDataSet(String name) {
    return this.datasets.containsKey(name) || this.datasetsV2.containsKey(name);
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   *  @param fabric the data fabric to inject
   *  @throws DataSetInstantiationException If failed to create the DataSet.
   */
  @SuppressWarnings("unchecked")
  public <T extends Closeable> T getDataSet(String dataSetName, DataFabric fabric,
                                            @Nullable DatasetFramework datasetFramework)
    throws DataSetInstantiationException {

    // find the data set specification
    DataSetSpecification spec = this.datasets.get(dataSetName);
    if (spec != null) {
      return (T) getDataSet(spec, fabric, dataSetName);
    }

    if (datasetFramework != null) {
      T dataSet = (T) getDataset(dataSetName, datasetFramework);
      if (dataSet != null) {
        return dataSet;
      }
    }

    throw logAndException(null, "No data set named %s can be instantiated.", dataSetName);
  }

  public <T extends Dataset> T getDataset(String dataSetName, DatasetFramework datasetFramework)
    throws DataSetInstantiationException {

    T dataset = getOrCreateDataset(dataSetName, datasetFramework);

    if (dataset instanceof TransactionAware) {
      txAware.add((TransactionAware) dataset);
      txAwareToMetricNames.put((TransactionAware) dataset, dataSetName);
    }

    return dataset;
  }

  private <T extends Dataset> T getOrCreateDataset(String datasetName, DatasetFramework datasetFramework)
    throws DataSetInstantiationException {
    try {
      if (!datasetFramework.hasInstance(datasetName)) {
        DatasetCreationSpec creationSpec = datasetsV2.get(datasetName);
        if (creationSpec == null) {
          return null;
        }
        try {
          datasetFramework.addInstance(creationSpec.getTypeName(), creationSpec.getInstanceName(),
                                       creationSpec.getProperties());
        } catch (Exception e) {
          throw new DataSetInstantiationException("could not create dataset " + datasetName, e);
        }
      }

      Dataset dataset = datasetFramework.getDataset(datasetName, classLoader);
      if (dataset == null) {
        throw new DataSetInstantiationException("Attempted to create dataset " + datasetName +
                                                  " but still cannot access it");
      }

      return (T) dataset;

    } catch (Exception e) {
      throw new DataSetInstantiationException("could not create dataset " + datasetName, e);
    }
  }

  /**
   * Returns an immutable life Iterable of {@link TransactionAware} objects.
   */
  // NOTE: this is needed for now to minimize destruction of early integration of txds2
  public Iterable<TransactionAware> getTransactionAware() {
    return Iterables.unmodifiableIterable(txAware);
  }

  public void addTransactionAware(TransactionAware transactionAware) {
    txAware.add(transactionAware);
  }

  public void removeTransactionAware(TransactionAware transactionAware) {
    txAware.remove(transactionAware);
  }

  /**
   * Helper method to cast the created data set object to its correct class.
   * This method is to isolate the unchecked cast (it has to be unchecked
   * because T is a type parameter, we cannot do instanceof or isAssignableFrom
   * on type parameters...) into a small method, that we can annotate with a
   * SuppressWarnings of small scope.
   * @param o The object to be cast
   * @param clz Class of the object.
   * @param <T> The type to cast to
   * @return The cast object of type T
   * @throws DataSetInstantiationException if the cast fails.
   */
  @SuppressWarnings("unchecked")
  private <T extends DataSet> T convert(Object o, Class<?> clz)
    throws DataSetInstantiationException {
    try {
      return (T) o;
    } catch (ClassCastException e) {
      throw logAndException(e, "Incompatible assignment of %s to type %s", DataSet.class.getName(), clz.getName());
    }
  }


  /**
   * Creates a DataSet instance from the given specification.
   * @param spec The specification of the DataSet.
   * @param fabric {@link DataFabric} for accessing underlying data system.
   * @param metricName Name for emitting metrics.
   * @param <T> Type of the DataSet.
   * @return A new instance of DataSet of type T.
   * @throws DataSetInstantiationException If failed to create the DataSet.
   */
  private <T extends DataSet> T getDataSet(DataSetSpecification spec, DataFabric fabric, String metricName)
    throws DataSetInstantiationException {
    // Instantiate the DataSet class, perform DataSet fields injection and invoke initialize(DataSetSpecification).
    String className = spec.getType();
    try {
      return instantiate(spec, fabric, metricName);
    } catch (ClassNotFoundException e) {
      throw logAndException(e, "Data set class %s not found", className);

    } catch (IllegalAccessException e) {
      throw logAndException(e, "Unable to access fields or methods in data set class %s", className);

    } catch (NoSuchFieldException e) {
      throw logAndException(e, "Invalid data set field in %s", className);

    } catch (Exception e) {
      throw logAndException(e, "Exception while instantiating class %s.", className);
    }
  }

  /**
   * Instantiate a {@link DataSet} through the {@link DataSetSpecification}.
   * @return instance of requested {@link DataSet}
   */
  private <T extends DataSet> T instantiate(DataSetSpecification spec,
                                            DataFabric fabric, String metricName) throws Exception {

    Class<?> dsClass = ClassLoaders.loadClass(spec.getType(), classLoader, this);
    TypeToken<?> dsType = TypeToken.of(dsClass);
    T instance = convert(instantiatorFactory.get(dsType).create(), dsClass);

    Supplier<T> dataSetDelegate = injectDelegate(instance, fabric, metricName);
    T delegateInstance = dataSetDelegate == null ? null : dataSetDelegate.get();

    // If the current DataSet actually uses delegate, the field injection is set on the delegateInstance
    // as that's the one do the actual operation. The fields on the instance is not injected so that
    // any attempt to use them at runtime would result in NPE (which is desired).
    // The reason why the Field object is get from the instance class, but injected to the runtime class works
    // because Runtime class always extends from the instance class.
    T setFieldInstance = delegateInstance == null ? instance : delegateInstance;

    // Inject DataSet and @Property fields.
    Reflections.visit(setFieldInstance, dsType,
                      new PropertyFieldSetter(spec.getProperties()),
                      new EmbeddedDataSetSetter(createDataSetContext(spec, fabric, metricName)));

    // Initialize delegate first
    if (delegateInstance != null) {
      initialize(delegateInstance, spec, createUserDataSetContext(spec, fabric, metricName));
    }

    initialize(instance, spec, createUserDataSetContext(spec, fabric, metricName));

    // TODO: This is the hack to get ocTable used inside RuntimeTable inside TxAware set.
    if (delegateInstance instanceof RuntimeTable) {
      TransactionAware txAware = ((RuntimeTable) delegateInstance).getTxAware();
      if (txAware != null) {
        this.txAware.add(txAware);
        this.txAwareToMetricNames.put(txAware, metricName);
      }
    }

    return instance;
  }

  /**
   * Creates actual table implementation supplier if the DataSet is one of those special tables
   * Doing this avoid exposing internal classes into public API.
   */
  private <T extends DataSet> Supplier<T> injectDelegate(DataSet dataSet, DataFabric fabric, String metricName)
    throws NoSuchFieldException, IllegalAccessException {
    Class<?> dsClass = dataSet.getClass();
    final Object delegate;
    final Class<?> delegateClass;

    // Construct corresponding Runtime DataSet. A bit hacky here as it has to list out all known Runtime type.
    if (MemoryTable.class.isAssignableFrom(dsClass)) {
      delegate = new RuntimeMemoryTable(fabric, metricName);
      delegateClass = Table.class;

    } else if (Table.class.isAssignableFrom(dsClass)) {
      delegate = new RuntimeTable(fabric, metricName);
      delegateClass = Table.class;

    } else if (FileDataSet.class.isAssignableFrom(dsClass)) {
      delegate = new RuntimeFileDataSet(fabric, metricName);
      delegateClass = FileDataSet.class;

    } else if (ObjectStore.class.isAssignableFrom(dsClass)) {
      delegate = RuntimeObjectStore.create(classLoader);
      delegateClass = ObjectStore.class;

    } else if (MultiObjectStore.class.isAssignableFrom(dsClass)) {
      delegate = RuntimeMultiObjectStore.create(classLoader);
      delegateClass = MultiObjectStore.class;

    } else {
      // No Runtime DataSet needs to inject
      return null;
    }

    // Construct the Supplier for injection
    final T instance = convert(delegate, dsClass);
    Supplier<T> supplier = new Supplier<T>() {
      @Override
      public T get() {
        return instance;
      }
    };

    // Find the field to inject to. The fields needs to be of type Supplier<DelegateClass>
    Field delegateField = Fields.findField(TypeToken.of(dsClass), "delegate", new Predicate<Field>() {
      @Override
      public boolean apply(Field field) {
        if (!Supplier.class.equals(field.getType())) {
          return false;
        }
        Type fieldType = field.getGenericType();
        if (!(fieldType instanceof ParameterizedType)) {
          return false;
        }

        Type[] typeArgs = ((ParameterizedType) fieldType).getActualTypeArguments();
        return typeArgs.length == 1 && delegateClass.equals(TypeToken.of(typeArgs[0]).getRawType());
      }
    });
    delegateField.setAccessible(true);
    delegateField.set(dataSet, supplier);

    return supplier;
  }

  /**
   * Calls the {@link DataSet#initialize(DataSetSpecification, DataSetContext)} method of the given DataSet.
   */
  private void initialize(DataSet dataSet, DataSetSpecification spec, DataSetContext context) {
    try {
      dataSet.initialize(spec, context);
    } catch (Throwable t) {
      throw logAndException(t, "Failed to initialize DataSet %s of name %s", dataSet.getClass(), spec.getName());
    }
  }

  /**
   * Creates a {@link DataSetContext} for initializing DataSet.
   */
  private DataSetContext createDataSetContext(final DataSetSpecification dataSetSpec,
                                              final DataFabric dataFabric, final String metricName) {
    return new DataSetContext() {
      @Override
      public <T extends Closeable> T getDataSet(String dataSetName) throws DataSetInstantiationException {
        DataSetSpecification spec = dataSetSpec.getSpecificationFor(dataSetName);
        if (spec == null) {
          throw logAndException(null, "No data set named %s declared for application.", dataSetName);
        }
        return (T) DataSetInstantiationBase.this.getDataSet(spec, dataFabric, metricName);
      }
    };
  }

  /**
   * Creates a {@link DataSetContext} for initializing user specified DataSet.
   */
  private DataSetContext createUserDataSetContext(DataSetSpecification spec, DataFabric dataFabric, String metricName) {
    final DataSetContext delegate = createDataSetContext(spec, dataFabric, metricName);
    return new DataSetContext() {
      @Override
      public <T extends Closeable> T getDataSet(String name) throws DataSetInstantiationException {
        // For non field injected DataSet, the name is prefixed with ".". See DataSetSpecification.
        String key = "." + name;
        return delegate.getDataSet(key);
      }
    };
  }

  /**
   * Helper method to log a message and create an exception. The caller is
   * responsible for throwing the exception.
   */
  private DataSetInstantiationException logAndException(Throwable e, String message, Object... params)
    throws DataSetInstantiationException {
    String msg;
    DataSetInstantiationException exn;
    if (e == null) {
      msg = String.format("Error instantiating data set: %s", String.format(message, params));
      exn = new DataSetInstantiationException(msg);
      LOG.error(msg);
    } else {
      msg = String.format("Error instantiating data set: %s. %s", String.format(message, params), e.getMessage());
      if (e instanceof DataSetInstantiationException) {
        exn = (DataSetInstantiationException) e;
      } else {
        exn = new DataSetInstantiationException(msg, e);
      }
      LOG.error(msg, e);
    }
    return exn;
  }

  public void setMetricsCollector(final MetricsCollectionService metricsCollectionService,
                                  final MetricsCollector programContextMetrics) {

    final MetricsCollector dataSetMetrics =
      metricsCollectionService.getCollector(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");

    for (Map.Entry<TransactionAware, String> txAware : this.txAwareToMetricNames.entrySet()) {
      // "old" datasets API
      if (txAware.getKey() instanceof DataSetClient) {
        final String dataSetName = txAware.getValue();
        DataSetClient.DataOpsMetrics dataOpsMetrics = new DataSetClient.DataOpsMetrics() {
          @Override
          public void recordRead(int opsCount) {
            // these metrics are within the context of the program that is performing the read
            // if the application is deleted, these metrics will be deleted as well
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.reads", 1, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.reads", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }

          @Override
          public void recordWrite(int opsCount, int dataSize) {
            // these metrics are within the context of the program that is performing the write
            // if the application is deleted, these metrics will be deleted as well
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.writes", 1, dataSetName);
              programContextMetrics.gauge("store.bytes", dataSize, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.writes", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.bytes", dataSize, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }
        };

        ((DataSetClient) txAware.getKey()).setMetricsCollector(dataOpsMetrics);
      }

      // datasets API V2
      if (txAware.getKey() instanceof MeteredDataset) {
        // TODO: fix namespacing: we want to capture metrics of namespaced dataset name - see REACTOR-217
        final String dataSetName = txAware.getValue();
        MeteredDataset.MetricsCollector metricsCollector = new MeteredDataset.MetricsCollector() {
          @Override
          public void recordRead(int opsCount, int dataSize) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.reads", 1, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.reads", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }

          @Override
          public void recordWrite(int opsCount, int dataSize) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.writes", 1, dataSetName);
              programContextMetrics.gauge("store.bytes", dataSize, dataSetName);
              programContextMetrics.gauge("store.ops", 1, dataSetName);
            }
            // these metrics are outside the context of any application and will stay unless explicitly
            // deleted.  Useful for dataset metrics that must survive the deletion of application metrics.
            if (dataSetMetrics != null) {
              dataSetMetrics.gauge("dataset.store.writes", 1, dataSetName);
              dataSetMetrics.gauge("dataset.store.bytes", dataSize, dataSetName);
              dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
            }
          }
        };

        ((MeteredDataset) txAware.getKey()).setMetricsCollector(metricsCollector);
      }
    }
  }
}
