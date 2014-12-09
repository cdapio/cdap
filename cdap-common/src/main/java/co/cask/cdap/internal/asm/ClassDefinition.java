/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Type;

import java.util.List;

/**
 * Class for carrying information of the generated class.
 */
public final class ClassDefinition {
  private final byte[] bytecode;
  private final String internalName;
  private final List<Class<?>> preservedClasses;

  public ClassDefinition(byte[] bytecode, String internalName) {
    this(bytecode, internalName, ImmutableList.<Class<?>>of());
  }

  public ClassDefinition(byte[] bytecode, String internalName, Iterable<? extends Class<?>> preservedClasses) {
    this.bytecode = bytecode;
    this.internalName = internalName;
    this.preservedClasses = ImmutableList.copyOf(preservedClasses);
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

  /**
   * Returns list of Classes that are used by the generated class which should be used as is,
   * instead of loaded again by the {@link ByteCodeClassLoader}.
   */
  public List<Class<?>> getPreservedClasses() {
    return preservedClasses;
  }
}
