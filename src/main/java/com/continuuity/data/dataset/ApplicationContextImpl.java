package com.continuuity.data.dataset;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ApplicationContextImpl
    implements ApplicationContext, ApplicationContextBuilder {

  private DataFabric fabric;
  private BatchCollectionClient collectionClient;

  private Map<String, DataSetMeta> datasets =
      new HashMap<String, DataSetMeta>();

  void logAndThrow(Throwable e, String message, Object... params)
      throws DataSetInstantiationException {
    if (e == null) {
      String msg = String.format("Error instantiating data set: %s.",
          String.format(message, params));
      // TODO log the message;
      throw new DataSetInstantiationException(msg);
    } else {
      String msg = String.format("Error instantiating data set: %s. %s",
        String.format(message, params), e.getMessage());
      // TODO log the message;
      throw new DataSetInstantiationException(msg, e);
    }
  }

  public
  <T extends DataSet> T getDataSet(String name)
      throws DataSetInstantiationException, OperationException {

    // find the data set meta data
    DataSetMeta meta = this.datasets.get(name);
    if (meta == null) {
      logAndThrow(null, "No data set named %s declared for application.", name);
      return null; // unreachable but code inspection does not know
    }

    String className = meta.getType();
    Class<?> dsClass;
    try {
      dsClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      logAndThrow(e, "Data set class %s not found", className);
      return null; // unreachable but code inspection does not know
    }

    Object ds = null;
    try {
      ds = dsClass.getConstructor(DataSetMeta.class).newInstance(meta);

    } catch (InvocationTargetException e) {
      logAndThrow(e.getTargetException(), "Exception from constructor for %s", className);

    } catch (NoSuchMethodException e) {
      logAndThrow(e, "Data set class %s does not declare constructor from DataSetMeta", className);

    } catch (InstantiationException e) {
      logAndThrow(e, "Data set class %s is not instantiable", className);

    } catch (IllegalAccessException e) {
      logAndThrow(e, "Constructor from DataSetMeta is not accessible in data set class %s", className);
    }
    this.injectDataFabric(ds);
    return this.convert(ds, className);
  }

  @SuppressWarnings("unchecked")
  private <T extends DataSet> T convert(Object o, String className)
      throws DataSetInstantiationException {
    try {
      return (T)o;
    }
    catch (ClassCastException e) {
      logAndThrow(e, "Incompatible assignment of dataset of type %s", className);
      return null; // unreachable but Java does not know
    }
  }

  private void injectDataFabric(Object obj)
      throws DataSetInstantiationException, OperationException {
    // for base data set types, directly inject the df fields
    if (obj instanceof Table) {
      Table table = (Table)obj;
      injectFields(table);
      table.open();
      return;
    }
    // otherwise recur through all fields of type DataSet
    Class<?> objClass = obj.getClass();
    for (Field field : objClass.getDeclaredFields()) {
      if (DataSet.class.isAssignableFrom(field.getType())) {
        field.setAccessible(true);
        Object fieldValue = null;
        try {
          fieldValue = field.get(obj);
        } catch (IllegalAccessException e) {
          logAndThrow(e, "Cannot access field %s of data set class %s", field.getName(), objClass.getName());
        }
        injectDataFabric(fieldValue);
      }
    }
  }

  private void injectFields(Table table)
      throws DataSetInstantiationException {
    // inject the data fabric
    try {
      Field dataFabricField = Table.class.getDeclaredField("dataFabric");
      dataFabricField.setAccessible(true);
      dataFabricField.set(table, this.fabric);
    } catch (Exception e) {
      logAndThrow(e, "Cannot access field dataFabric of class Table");
    }
    // inject the batch collection client
    try {
      Field collectionField = Table.class.getDeclaredField("collectionClient");
      collectionField.setAccessible(true);
      collectionField.set(table, this.collectionClient);
    } catch (Exception e) {
      logAndThrow(e, "Cannot access field collectionClient of class Table");
    }
  }

  @Override
  public void setBatchCollectionClient(BatchCollectionClient client) {
    this.collectionClient = client;
  }

  @Override
  public void setDataFabric(DataFabric fabric) {
    this.fabric = fabric;
  }

  @Override
  public void addDataSet(DataSetMeta meta) {
    datasets.put(meta.getName(), meta);
  }

}

