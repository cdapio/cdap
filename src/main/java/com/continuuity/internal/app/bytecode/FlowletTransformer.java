package com.continuuity.internal.app.bytecode;

import java.util.List;

/**
 * Inspect and transform a flowlet bytecode
 *
 * - Creates public default constructor if it is missing.
 * - Creates a public inner class for dispatching calls to process method.
 *
 */
public class FlowletTransformer {

  interface ClassByteCode {

    /**
     * @return Internal class name of the class (e.g. com/continuuity/api/flow/flowlet/Flowlet)
     */
    String getClassInternalName();

    /**
     * @return byte code of the class
     */
    byte[] getByteCode();
  }


  /**
   * @param bytecode Origin Flowlet bytecode
   * @return A List of {@link ClassByteCode} for the set of class and bytecode association.
   */
  public List<ClassByteCode> transform(byte[] bytecode) {
    return null;
  }
}
