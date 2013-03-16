package com.continuuity.internal.asm;

import com.continuuity.archive.MultiClassLoader;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * A private {@link ClassLoader} for loading generated {@link com.continuuity.internal.io.DatumWriter} bytecode.
 */
public class ByteCodeClassLoader extends MultiClassLoader {

  protected final Map<String, byte[]> bytecodes;

  public ByteCodeClassLoader(ClassLoader parent) {
    super(parent);
    bytecodes = Maps.newHashMap();
  }

  public final synchronized ByteCodeClassLoader addClass(ClassDefinition classDef) {
    bytecodes.put(classDef.getClassName(), classDef.getBytecode());
    return this;
  }

  @Override
  protected final synchronized byte[] loadClassBytes(String className) {
    return bytecodes.get(className);
  }
}
