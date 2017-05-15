/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore;

import co.cask.cdap.explore.service.ExploreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hive.service.cli.thrift.TColumnValue;

import java.lang.reflect.InvocationTargetException;

/**
 * Utility methods to invoke corresponding Hive methods in a compatible manner
 */
public class HiveUtilities {
  private HiveUtilities() {
    // to prevent object creation
  }

  /**
   * Deserialize the Hive expression by invoking the right method based on the Hive version used.
   * This is needed for compatibility across various Hive versions.
   */
  public static ExprNodeGenericFuncDesc deserializeExpression(String serializedExpr, Configuration conf) {
    // Hack to deal with the fact that older versions of Hive 1.x use
    // Utilities.deserializeExpression(String, Configuration),
    // whereas newer versions of Hive 1.x use Utilities.deserializeExpression(String).
    // Hive 2.x uses SerializationUtilities.deserializeExpression(serializedExpr)
    ExprNodeGenericFuncDesc expr;
    try {
      // If SerializationUtilities class is present, use that
      Class<?> serializationUtilitiesClass =
        HiveUtilities.class.getClassLoader().loadClass("org.apache.hadoop.hive.ql.exec.SerializationUtilities");
      expr = (ExprNodeGenericFuncDesc) serializationUtilitiesClass.getMethod("deserializeExpression", String.class)
        .invoke(null, serializedExpr);
    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      try {
        // Else try Utilities.deserializeExpression(String)
        expr = Utilities.deserializeExpression(serializedExpr);
      } catch (NoSuchMethodError e2) {
        try {
          // Else try SerializationUtilities.deserializeExpression(serializedExpr)
          expr = (ExprNodeGenericFuncDesc) Utilities.class.getMethod(
            "deserializeExpression", String.class, Configuration.class).invoke(null, serializedExpr, conf);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e1) {
          throw new IllegalStateException("Not able to deserialize expression. The Hive version is not supported.", e1);
        }
      }
    }
    return expr;
  }

  /**
   * Converts TColumnValue objects into their corresponding objects. This is needed for compatibility across
   * various Hive versions
   */
  public static Object tColumnToObject(TColumnValue tColumnValue) throws ExploreException {
    if (tColumnValue.isSetBoolVal()) {
      return tColumnValue.getBoolVal().isValue();
    } else if (tColumnValue.isSetByteVal()) {
      return tColumnValue.getByteVal().getValue();
    } else if (tColumnValue.isSetDoubleVal()) {
      return tColumnValue.getDoubleVal().getValue();
    } else if (tColumnValue.isSetI16Val()) {
      return tColumnValue.getI16Val().getValue();
    } else if (tColumnValue.isSetI32Val()) {
      return tColumnValue.getI32Val().getValue();
    } else if (tColumnValue.isSetI64Val()) {
      return tColumnValue.getI64Val().getValue();
    } else if (tColumnValue.isSetStringVal()) {
      return tColumnValue.getStringVal().getValue();
    }
    throw new ExploreException("Unknown column value encountered: " + tColumnValue);
  }
}
