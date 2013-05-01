package com.continuuity.internal.asm;

import org.objectweb.asm.Type;

/**
 * Class for carrying information of the generated class.
 */
public final class ClassDefinition {
  private final byte[] bytecode;
  private final String internalName;

  public ClassDefinition(byte[] bytecode, String internalName) {
    this.bytecode = bytecode;
    this.internalName = internalName;
  }

  public byte[] getBytecode() {
    return bytecode;
  }

  public String getInternalName() {
    return internalName;
  }

  public String getClassName() {
    return Type.getObjectType(internalName).getClassName();
  }
}
