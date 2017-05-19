/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.artifact;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link ClassLoader} that implements {@link Closeable} for resource cleanup. All classloading is done
 * by the delegate {@link ClassLoader}.
 */
public class CloseableClassLoader extends ClassLoader implements Closeable {

  private final Closeable closeable;

  public CloseableClassLoader(ClassLoader delegate, Closeable closeable) {
    super(delegate);
    this.closeable = closeable;
  }

  @Override
  public void close() throws IOException {
    closeable.close();
  }
}
