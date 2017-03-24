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

package co.cask.cdap.common.lang;

import java.io.IOException;
import java.io.InputStream;

/**
 * Represents classes that can rewrite class bytecode.
 */
public interface ClassRewriter {

  /**
   * Rewrites a class with the original byte code provided from the given {@link InputStream}.
   *
   * @param className name of the class
   * @param input an {@link InputStream} to provide the original bytecode of the class
   * @return the bytecode of the rewritten class
   * @throws IOException if failed in rewriting the class
   */
  byte[] rewriteClass(String className, InputStream input) throws IOException;
}
