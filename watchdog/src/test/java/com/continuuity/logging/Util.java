package com.continuuity.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.google.common.base.Throwables;

import java.lang.reflect.Field;

/**
 * Util class to create local opex.
 */
public class Util {
  public static OperationExecutor getOpex() {
    try {
      MemoryOracle memoryOracle = new MemoryOracle();
      injectField(memoryOracle.getClass(), memoryOracle, "timeOracle", new MemoryStrictlyMonotonicTimeOracle());
      return new OmidTransactionalOperationExecutor(memoryOracle, MemoryOVCTableHandle.getInstance(),
                                             new CConfiguration());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static void injectField(Class<?> cls, Object obj, String fieldName, Object fieldValue) throws Exception{
    Field field = cls.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, fieldValue);
  }
}
