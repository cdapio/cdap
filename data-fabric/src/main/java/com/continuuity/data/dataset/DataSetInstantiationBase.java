package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.FileDataSet;
import com.continuuity.api.data.dataset.MultiObjectStore;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.data.DataFabric;
import com.continuuity.data2.RuntimeTable;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.internal.io.InstantiatorFactory;
import com.continuuity.internal.lang.ClassLoaders;
import com.continuuity.internal.reflect.Fields;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the core logic of instantiating data set, including injection of the data fabric runtime and
 * built-in data sets.
 */
public class DataSetInstantiationBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataSetInstantiationBase.class);

  // the class loader to use for data set classes
  private final ClassLoader classLoader;
  // the known data set specifications
  private final Map<String, DataSetSpecification> datasets = Maps.newHashMap();

  private final Set<TransactionAware> txAware = Sets.newIdentityHashSet();
  // in this collection we have only datasets initialized with getDataSet() which is OK for now...
  private final Map<TransactionAware, String> txAwareToMetricNames = Maps.newIdentityHashMap();

  private final InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

  public DataSetInstantiationBase() {
    this(null);
  }

  public DataSetInstantiationBase(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  /**
   * Set the data set spec for all data sets that this instantiator can
   * create. This should be a list of DataSetSpecification's obtained from actual
   * data sets' configure() method.
   * @param specs The list of DataSetSpecification's
   */
  public void setDataSets(Iterable<DataSetSpecification> specs) {
    for (DataSetSpecification spec : specs) {
      this.datasets.put(spec.getName(), spec);
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
    return this.datasets.containsKey(name);
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   *  @param fabric the data fabric to inject
   */
  public <T extends DataSet> T getDataSet(String dataSetName, DataFabric fabric)
    throws DataSetInstantiationException {

    // find the data set specification
    DataSetSpecification spec = this.datasets.get(dataSetName);
    if (spec == null) {
      throw logAndException(null, "No data set named %s declared for application.", dataSetName);
    }

    // determine the class of this data set
    // invoke the DataSet(DataSetSpecification) constructor for that class.
    // this yields an object (Java does not know that this is a DataSet)
    String className = spec.getType();
    try {
      return instantiate(spec, fabric, dataSetName);
    } catch (ClassNotFoundException e) {
      throw logAndException(e, "Data set class %s not found", className);

    } catch (InvocationTargetException e) {
      throw logAndException(e.getTargetException(), "Exception from constructor for %s", className);

    } catch (NoSuchMethodException e) {
      throw logAndException(e, "Data set class %s does not declare constructor from DataSetSpecification", className);

    } catch (InstantiationException e) {
      throw logAndException(e, "Data set class %s is not instantiable", className);

    } catch (IllegalAccessException e) {
      throw logAndException(
        e, "Constructor from DataSetSpecification is not accessible in data set class %s", className);
    } catch (NoSuchFieldException e) {
      throw logAndException(
        e, "Invalid data set field in %s", className);
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
      throw logAndException(e, "Incompatible assignment of %s of type %s", DataSet.class.getName(), clz.getName());
    }
  }


  /**
   * Creates actual table implementation supplier if the DataSet is one of those special tables
   * Doing this avoid exposing internal classes into public API.
   */
  private <T extends DataSet> Supplier<T> createDataSetDelegate(Class<?> dsClass,
                                                                DataFabric fabric,
                                                                String metricName) {
    Object instance = null;
    if (Table.class.equals(dsClass)) {
      instance = new RuntimeTable(fabric, metricName);
    } else if (FileDataSet.class.equals(dsClass)) {
      instance = new RuntimeFileDataSet(fabric, metricName);
    } else if (ObjectStore.class.isAssignableFrom(dsClass)) {
      instance = RuntimeObjectStore.create(classLoader);
    } else if (MultiObjectStore.class.isAssignableFrom(dsClass)) {
      instance = RuntimeMultiObjectStore.create(classLoader);
    }

    if (instance != null) {
      final T finalInstance = convert(instance, dsClass);
      return new Supplier<T>() {
        @Override
        public T get() {
          return finalInstance;
        }
      };
    }

    return null;
  }

  /**
   * Instantiate a {@link DataSet} through the {@link DataSetSpecification}.
   * @param spec
   * @param fabric
   * @param metricName
   * @param <T>
   * @return
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws ClassNotFoundException
   * @throws DataSetInstantiationException
   */
  private <T extends DataSet> T instantiate(DataSetSpecification spec, DataFabric fabric, String metricName)
                                                       throws NoSuchMethodException, IllegalAccessException,
                                                              InvocationTargetException, InstantiationException,
                                                              ClassNotFoundException, DataSetInstantiationException,
                                                              NoSuchFieldException {

    Class<?> dsClass = ClassLoaders.loadClass(spec.getType(), classLoader, this);
    T instance = convert(instantiatorFactory.get(TypeToken.of(dsClass)).create(), dsClass);

    Supplier<T> dataSetDelegate = createDataSetDelegate(dsClass, fabric, metricName);
    T delegateInstance = dataSetDelegate == null ? null : dataSetDelegate.get();

    // Inject DataSet fields.
    for (Class<?> type : TypeToken.of(dsClass).getTypes().classes().rawTypes()) {
      if (Object.class.equals(type)) {
        break;
      }

      for (Field field : type.getDeclaredFields()) {
        if (!DataSet.class.isAssignableFrom(field.getType())) {
          continue;
        }

        if (!field.isAccessible()) {
          field.setAccessible(true);
        }

        DataSetSpecification fieldDataSetSpec = spec.getSpecificationFor(field.getName());
        if (fieldDataSetSpec == null) {
          throw logAndException(null, "Failed to get DataSetSpecification for %s in class %s.",
                                field.getName(), dsClass.getName());
        }

        DataSet dataSetField = instantiate(fieldDataSetSpec, fabric, metricName);
        field.set(delegateInstance == null ? instance : delegateInstance, dataSetField);
      }
    }

    initialize(instance, spec);

    // Initialize and inject delegate
    if (delegateInstance != null) {
      delegateInstance.initialize(spec);

      // TODO: This is the hack to get ocTable used inside RuntimeTable inside TxAware set.
      if (delegateInstance instanceof RuntimeTable) {
        TransactionAware txAware = ((RuntimeTable) delegateInstance).getTxAware();
        if (txAware != null) {
          this.txAware.add(txAware);
          this.txAwareToMetricNames.put(txAware, metricName);
        }
      }

      Field delegateField = Fields.findField(TypeToken.of(dsClass), "delegate", new Predicate<Field>() {
        @Override
        public boolean apply(Field input) {
          return Supplier.class.isAssignableFrom(input.getType());
        }
      });
      delegateField.setAccessible(true);
      delegateField.set(instance, dataSetDelegate);
    }

    return instance;
  }

  private void initialize(DataSet dataSet, DataSetSpecification spec) {
    try {
      dataSet.initialize(spec);
    } catch (Throwable t) {
      throw logAndException(t, "Failed to initialize DataSet %s of name %s", dataSet.getClass(), spec.getName());
    }
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
      msg = String.format("Error instantiating data set: %s.", String.format(message, params));
      exn = new DataSetInstantiationException(msg);
      LOG.error(msg);
    } else {
      msg = String.format("Error instantiating data set: %s. %s", String.format(message, params), e.getMessage());
      exn = new DataSetInstantiationException(msg, e);
      LOG.error(msg, e);
    }
    return exn;
  }

  public void setMetricsCollector(final MetricsCollector programContextMetrics) {

    for (Map.Entry<TransactionAware, String> txAware : this.txAwareToMetricNames.entrySet()) {
      if (txAware.getKey() instanceof DataSetClient) {
        final String dataSetName = txAware.getValue();
        DataSetClient.DataOpsMetrics dataOpsMetrics = new DataSetClient.DataOpsMetrics() {
          @Override
          public void recordRead(int opsCount) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.reads", 1, dataSetName);
              programContextMetrics.gauge("store.ops", 1);
            }
          }

          @Override
          public void recordWrite(int opsCount, int dataSize) {
            if (programContextMetrics != null) {
              programContextMetrics.gauge("store.writes", 1, dataSetName);
              programContextMetrics.gauge("store.bytes", dataSize, dataSetName);
              programContextMetrics.gauge("store.ops", 1);
            }
          }
        };

        ((DataSetClient) txAware.getKey()).setMetricsCollector(dataOpsMetrics);
      }
    }
  }
}
