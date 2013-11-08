package com.continuuity.data2.util.hbase;

import com.continuuity.weave.internal.utils.Instances;
import com.google.common.base.Throwables;
import com.google.inject.Provider;

/**
 *
 */
public class HBaseTableUtilFactory implements Provider<HBaseTableUtil> {
  /*
   * Hard-coding the classnames here is a necessary evil to avoid a cyclical dependency of
   * hbase-compat-* -> data-fabric -> hbase-compat-*.
   */
  private static final String HBASE_94_CLASSNAME = "com.continuuity.data2.util.hbase.HBase94TableUtil";
  private static final String HBASE_96_CLASSNAME = "com.continuuity.data2.util.hbase.HBase96TableUtil";

  @Override
  public HBaseTableUtil get() {
    HBaseTableUtil util = null;
    try {
      switch (HBaseVersion.get()) {
        case HBASE_94:
          util = createInstance(HBASE_94_CLASSNAME);
          break;
        case HBASE_96:
          util = createInstance(HBASE_96_CLASSNAME);
          break;
        case UNKNOWN:
          throw new IllegalStateException("Unknown HBase version: " + HBaseVersion.getVersionString());
      }
    } catch (ClassNotFoundException cnfe) {
      throw Throwables.propagate(cnfe);
    }
    return util;
  }

  private HBaseTableUtil createInstance(String className) throws ClassNotFoundException {
    Class clz = Class.forName(className);
    return (HBaseTableUtil) Instances.newInstance(clz);
  }
}
