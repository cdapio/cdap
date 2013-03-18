package com.continuuity.internal.asm;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Util class for debugging ASM bytecode generation.
 */
public final class Debugs {

  public static void debugByteCode(ClassDefinition classDefinition, PrintWriter writer) {
    ClassReader reader = new ClassReader(classDefinition.getBytecode());
    reader.accept(new CheckClassAdapter(new TraceClassVisitor(writer)), 0);

    File file = new File("/tmp/" + classDefinition.getInternalName() + ".class");
    file.getParentFile().mkdirs();
    writer.println(file);
    writer.flush();
    try {
      ByteStreams.write(classDefinition.getBytecode(), Files.newOutputStreamSupplier(file));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Debugs() {}
}
