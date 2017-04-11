/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.guice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A {@link LocationFactory} in distributed mode that is only used in insecure cluster.
 * It uses the same {@link FileContext} for every location.
 */
public class InsecureFileContextLocationFactory extends FileContextLocationFactory {

  private final FileContext fileContext;

  public InsecureFileContextLocationFactory(Configuration configuration, String pathBase, FileContext fileContext) {
    super(configuration, pathBase);
    this.fileContext = fileContext;
  }

  @Override
  public FileContext getFileContext() {
    return fileContext;
  }
}
