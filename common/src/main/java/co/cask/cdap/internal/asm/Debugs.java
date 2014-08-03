/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.asm;

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
