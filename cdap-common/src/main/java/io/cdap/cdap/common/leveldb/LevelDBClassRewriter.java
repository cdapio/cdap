/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.leveldb;

import io.cdap.cdap.common.lang.ClassRewriter;
import java.io.IOException;
import java.io.InputStream;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;

/**
 * Current java LevelDB implementation has a memory accumulation problem during compaction. Each
 * {@link org.iq80.leveldb.impl.FileMetaData} object holds minimum and maximum keys for the file.
 * And those keys may reside in a slice pointing to a byte array with the whole file. In this case
 * instead of holding 20 bytes reference, we hold a multi-kb or even mb reference per file, leading
 * to OOM on big compaction. The patch checks all {@link org.iq80.leveldb.impl.InternalKey} objects
 * while they are created and if it received such heavy metadata reference, makes a copy of the key
 * (usually few dozen bytes) and uses this lightweight metadata reference instead.
 */
public class LevelDBClassRewriter implements ClassRewriter {

  /**
   * Full filename of the {@link org.iq80.leveldb.impl.FileMetaData} class to be patched
   */
  private static final String FILE_META_DATA_CLASS_NAME = "org.iq80.leveldb.impl.FileMetaData";
  /**
   * Type of {@link FileMetaDataUtil} class that has normalization static methos to trim key storage
   * down
   */
  private static final Type FILE_META_DATA_UTIL_TYPE = Type.getType(FileMetaDataUtil.class);
  /**
   * {@link Type} of {@link org.iq80.leveldb.impl.InternalKey}
   */
  private static final Type INTERNAL_KEY_TYPE = Type.getObjectType(
      "org/iq80/leveldb/impl/InternalKey");
  /**
   * {@link org.iq80.leveldb.impl.FileMetaData#FileMetaData} {@link Method}
   */
  public static final Method FILE_META_DATA_CONSTRUCTOR = new Method("<init>", Type.VOID_TYPE,
      new Type[]{
          Type.LONG_TYPE, Type.LONG_TYPE, INTERNAL_KEY_TYPE, INTERNAL_KEY_TYPE});
  /**
   * Parameter number for "smallest" parameter in {@link org.iq80.leveldb.impl.FileMetaData#FileMetaData}
   */
  public static final int SMALLEST_KEY_PARAM = 2;
  /**
   * Parameter number for "largest" parameter in {@link org.iq80.leveldb.impl.FileMetaData#FileMetaData}
   */
  public static final int LARGEST_KEY_PARAM = 3;
  /**
   * {@link FileMetaDataUtil#normalize} {@link Method}
   */
  private static final Method NORMALIZE_METHOD =
      new Method("normalize", Type.getMethodDescriptor(INTERNAL_KEY_TYPE, INTERNAL_KEY_TYPE));

  /**
   * @param className class name inspected
   * @return true if thi is {@link org.iq80.leveldb.impl.FileMetaData} class that needs to be
   *     rewritten
   */
  public boolean needRewrite(String className) {
    return FILE_META_DATA_CLASS_NAME.equals(className);
  }

  /**
   * Rewrites {@link org.iq80.leveldb.impl.FileMetaData#FileMetaData(long, long, InternalKey,
   * InternalKey)} constructor to trim down heavy storage for keys passed as 3rd and 4th parameter.
   *
   * @param className name of the class - must be FileMetaData
   * @param input an {@link InputStream} to provide the original bytecode of the class
   * @return rewritten class bytecode
   * @throws IOException if class can't be read
   */
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    cr.accept(new ClassVisitor(Opcodes.ASM7, cw) {
      @Override
      public MethodVisitor visitMethod(int access, String name, String descriptor,
          String signature, String[] exceptions) {

        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
        if (!FILE_META_DATA_CONSTRUCTOR.equals(new Method(name, descriptor))) {
          return mv;
        }
        return new AdviceAdapter(Opcodes.ASM7, mv, access, name, descriptor) {
          @Override
          protected void onMethodEnter() {
            loadArg(SMALLEST_KEY_PARAM);
            invokeStatic(FILE_META_DATA_UTIL_TYPE, NORMALIZE_METHOD);
            storeArg(SMALLEST_KEY_PARAM);
            loadArg(LARGEST_KEY_PARAM);
            invokeStatic(FILE_META_DATA_UTIL_TYPE, NORMALIZE_METHOD);
            storeArg(LARGEST_KEY_PARAM);
          }
        };
      }
    }, 0);

    return cw.toByteArray();
  }

}
