package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.*;
import com.continuuity.api.data.dataset.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The data set instantiator creates instances of data sets at runtime. It
 * must be called from the execution context to get operational instances
 * of data sets. Given a list of data set specs and a data fabric runtime
 * (a data fabric and a batch collection client), it can construct an instance
 * of a data set and inject the data fabric runtime into its base tables (and
 * other built-in data sets).
 *
 * The instantiation and injection uses Java reflection a lot. This may look
 * unclean, but it helps us keep the DataSet API clean and simple (no need
 * to pass in data fabric runtime, no exposure of the developer to the raw
 * data fabric, he only interacts with data sets).
 */
public class DataSetInstantiator implements DataSetContext {

  private static final Logger Log =
      LoggerFactory.getLogger(DataSetInstantiator.class);

  // the data fabric (an operation executor and an operation context
  private DataFabric fabric;
  // the batch collection client
  private BatchCollectionClient collectionClient;

  private Map<String, DataSetSpecification> datasets =
      new HashMap<String, DataSetSpecification>();

  /**
   * Set the batch collection client. This must be shared with the execution
   * driver of the program (e.g. flow) where the data set will be used. This
   * batch collection client is injected into each com.continuuity.data.dataset, and the driver
   * (e.g. flowlet driver) must use it to change the batch collector for each
   * new transactions (e.g. a flowlet driver would set a new output collector
   * each tuple that is processed).
   * @param client The batch collection client to inject
   */
  public void setBatchCollectionClient(BatchCollectionClient client) {
    this.collectionClient = client;
  }

  /**
   * Set the Data Fabric to inject into each data set. The data fabric has
   * an operation context and the operation executor, and it is used for all
   * synchronous operations that a data set performs.
   * @param fabric the data fabric to inject
   */
  public void setDataFabric(DataFabric fabric) {
    this.fabric = fabric;
  }

  /**
   * Set the data set spec for all data sets that this instantiator can
   * create. This should be a list of DataSetSpecification's obtained from actual
   * data sets' configure() method.
   * @param specs The list of DataSetSpecification's
   */
  public void setDataSets(List<DataSetSpecification> specs) {
    for (DataSetSpecification spec : specs) {
      this.datasets.put(spec.getName(), spec);
    }
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   */
  public
  <T extends DataSet> T getDataSet(String dataSetName)
      throws DataSetInstantiationException, OperationException {

    // find the data set specification
    DataSetSpecification spec = this.datasets.get(dataSetName);
    if (spec == null) {
      throw logAndException(null, "No data set named %s declared for application.", dataSetName);
    }

    // determine the class of this data set
    String className = spec.getType();
    Class<?> dsClass;
    try {
      dsClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw logAndException(e, "Data set class %s not found", className);
    }

    // invoke the DataSet(DataSetSpecification) constructor for that class.
    // this yields an object (Java does not know that this is a DataSet)
    Object ds;
    try {
      ds = dsClass.getConstructor(DataSetSpecification.class).newInstance(spec);

    } catch (InvocationTargetException e) {
      throw logAndException(e.getTargetException(), "Exception from constructor for %s", className);

    } catch (NoSuchMethodException e) {
      throw logAndException(e, "Data set class %s does not declare constructor from DataSetSpecification", className);

    } catch (InstantiationException e) {
      throw logAndException(e, "Data set class %s is not instantiable", className);

    } catch (IllegalAccessException e) {
      throw logAndException(e, "Constructor from DataSetSpecification is not accessible in data set class %s", className);
    }

    // inject the data fabric runtime
    this.injectDataFabric(ds);

    // cast to the actual data set class and return
    return this.convert(ds, className);
  }

  /**
   * helper method to cast the created data set object to its correct class.
   * This method is to isolate the unchecked cast (it has to be unchecked
   * because T is a type parameter, we cannot do instanceof or isAssignableFrom
   * on type parameters...) into a small method, that we can annotate with a
   * SuppressWarnings of small scope.
   * @param o The object to be cast
   * @param className the name of the class of that object, for error messages
   * @param <T> The type to cast to
   * @return The cast object of type T
   * @throws DataSetInstantiationException if the cast fails.
   */
  @SuppressWarnings("unchecked")
  private <T extends DataSet> T convert(Object o, String className)
      throws DataSetInstantiationException {
    try {
      return (T)o;
    }
    catch (ClassCastException e) {
      throw logAndException(e, "Incompatible assignment of com.continuuity.data.dataset of type %s", className);
    }
  }

  /**
   * Inject the data fabric into every (directly or transitively) embedded
   * data set of the given object. We assume here that embedded data sets are
   * always direct members of the embedding data set.
   *
   * Why use reflection here? The alternative would be to add methods to the
   * data set API to set the data fabric runtime. The disadvantage of that is
   * that the raw data fabric would be exposed to the developer (which we do
   * not desire), the developer would be required to pass down those API calls
   * to the embedded data sets, and any change in the logic would require a
   * potential change of the API. Thus, even though reflection is "dirty" it
   * keeps the DataSet API itself clean.
   *
   * @param obj The com.continuuity.data.dataset to inject into
   * @throws DataSetInstantiationException If any of the reflection magic goes wrong
   * @throws OperationException If a table cannot to opened
   */
  private void injectDataFabric(Object obj)
      throws DataSetInstantiationException, OperationException {
    // for base data set types, directly inject the df fields
    if (obj instanceof Table) {
      // this sets the delegate table of the Table to a new CoreTable
      CoreTable coreTable = CoreTable.setCoreTable((Table)obj, this.fabric, this.collectionClient);
      // also ensure that the table exists in the data fabric
      coreTable.open();
      return;
    }
    // otherwise recur through all fields of type DataSet
    Class<?> objClass = obj.getClass();
    for (Field field : objClass.getDeclaredFields()) {
      if (DataSet.class.isAssignableFrom(field.getType())) {
        field.setAccessible(true);
        Object fieldValue;
        try {
          fieldValue = field.get(obj);
        } catch (IllegalAccessException e) {
          throw logAndException(e, "Cannot access field %s of data set class %s",
              field.getName(), objClass.getName());
        }
        injectDataFabric(fieldValue);
      }
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
    } else {
      msg = String.format("Error instantiating data set: %s. %s", String.format(message, params), e.getMessage());
      exn = new DataSetInstantiationException(msg, e);
    }
    Log.error(msg);
    return exn;
  }

}
