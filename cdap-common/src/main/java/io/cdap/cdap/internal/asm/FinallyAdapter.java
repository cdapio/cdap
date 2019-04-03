/*
 * Copyright © 2016 Cask Data, Inc.
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

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.commons.AdviceAdapter;

/**
 * A {@link MethodVisitor} that wraps method call with {@code try-finally} block.
 * Sub-class can override the {@link #onFinally(int)} method to insert custom code in the finally block.
 */
public class FinallyAdapter extends AdviceAdapter {

  private final Label startFinally = new Label();

  protected FinallyAdapter(int api, MethodVisitor mv, int access, String name, String desc) {
    super(api, mv, access, name, desc);
  }

  @Override
  public void visitMaxs(int maxStack,
                        int maxLocals) {
    Label endFinally = new Label();
    mv.visitTryCatchBlock(startFinally,
                          endFinally, endFinally, null);
    mv.visitLabel(endFinally);

    // this is essentially catching exception, invoke the finally code block and rethrow the exception
    onFinally(ATHROW);
    mv.visitInsn(ATHROW);
    mv.visitMaxs(maxStack, maxLocals);
  }

  @Override
  protected void onMethodExit(int opcode) {
    if (opcode != ATHROW) {
      onFinally(opcode);
    }
  }

  /**
   * Sub-class should call this to signal the beginning of the try block.
   */
  protected void beginTry() {
    mv.visitLabel(startFinally);
  }

  /**
   * Calls to generate code for the finally block.
   */
  protected void onFinally(int opcode) {
    // No-op
  }
}
