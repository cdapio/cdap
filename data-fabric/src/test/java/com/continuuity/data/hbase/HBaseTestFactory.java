package com.continuuity.data.hbase;

import com.continuuity.data2.util.hbase.HBaseVersion;
import com.continuuity.weave.internal.utils.Instances;
import com.google.common.base.Throwables;
import com.google.inject.Provider;

/**
 * Factory class to provide instances of the correct {@link HBaseTestBase} implementation, dependent on the version
 * of HBase that is being used.
 */
public class HBaseTestFactory implements Provider<HBaseTestBase> {
  private static final String HBASE_94_CLASSNAME = "com.continuuity.data.hbase.HBase94Test";
  private static final String HBASE_96_CLASSNAME = "com.continuuity.data.hbase.HBase96Test";

  @Override
  public HBaseTestBase get() {
    HBaseTestBase instance = null;
    try {
      switch (HBaseVersion.get()) {
        case HBASE_94:
          instance = createInstance(HBASE_94_CLASSNAME);
          break;
        case HBASE_96:
          instance = createInstance(HBASE_96_CLASSNAME);
          break;
        case UNKNOWN:
          throw new IllegalArgumentException("Unknown version of HBase: " + HBaseVersion.getVersionString());
      }
    } catch (ClassNotFoundException cnfe) {
      throw Throwables.propagate(cnfe);
    }
    return instance;
  }

  private HBaseTestBase createInstance(String className) throws ClassNotFoundException {
    Class clz = Class.forName(className);
    return (HBaseTestBase) Instances.newInstance(clz);
  }
}
