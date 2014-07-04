package com.continuuity.data2.util.hbase;

import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import org.apache.twill.internal.utils.Instances;

/**
 * Common class factory behavior for classes which need specific implementations depending on HBase versions.
 * Specific factories can subclass this class and simply plug in the class names for their implementations.
 *
 * @param <T> Version specific class provided by this factory.
 */
public abstract class HBaseVersionSpecificFactory<T> implements Provider<T> {
  @Override
  public T get() {
    T instance = null;
    try {
      switch (HBaseVersion.get()) {
        case HBASE_94:
          instance = createInstance(getHBase94Classname());
          break;
        case HBASE_96:
          instance = createInstance(getHBase96Classname());
          break;
        case HBASE_98:
          // for our needs HBase 0.98 is API compatible with 0.96
          instance = createInstance(getHBase96Classname());
          break;
        case UNKNOWN:
          throw new ProvisionException("Unknown HBase version: " + HBaseVersion.getVersionString());
      }
    } catch (ClassNotFoundException cnfe) {
      throw new ProvisionException(cnfe.getMessage(), cnfe);
    }
    return instance;
  }

  private T createInstance(String className) throws ClassNotFoundException {
    Class clz = Class.forName(className);
    return (T) Instances.newInstance(clz);
  }

  protected abstract String getHBase94Classname();
  protected abstract String getHBase96Classname();
}
