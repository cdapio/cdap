package com.continuuity.archive;

import com.continuuity.common.lang.MultiClassLoader;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

/**
 *
 */
public class ClassLoaderTest {

  @Test
  public void testClassLoader() throws ClassNotFoundException {
    final String recordClassName = "com.continuuity.TestRecord";

    ClassLoader classLoader = new MultiClassLoader(ClassLoader.getSystemClassLoader()) {
      @Override
      protected byte[] loadClassBytes(String className) {
        if (className.equals(recordClassName)) {
          Type classType = Type.getObjectType(recordClassName.replace('.', '/'));
          ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
          cw.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC, classType.getInternalName(), null,
                   Type.getInternalName(Object.class), new String[0]);
          GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC,
                                                     Method.getMethod("void <init> ()"), null, new Type[0], cw);
          mg.loadThis();
          mg.invokeConstructor(Type.getType(Object.class), Method.getMethod("void <init> ()"));
          mg.returnValue();
          mg.endMethod();

          return cw.toByteArray();
        }
        return null;
      }
    };

    Class<?> strClass = classLoader.loadClass("java.lang.String");
    Assert.assertEquals(String.class, strClass);

    Class<?> recordClass = classLoader.loadClass(recordClassName);
    Assert.assertSame(classLoader, recordClass.getClassLoader());

    Assert.assertSame(recordClass, classLoader.loadClass(recordClassName));
  }
}
