/*
 * Copyright © 2023 Cask Data, Inc.
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

<<<<<<<< HEAD:cdap-source-control/src/main/java/io/cdap/cdap/sourcecontrol/NoChangesToPushException.java
package io.cdap.cdap.sourcecontrol;
========
package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.sourcecontrol.RepositoryManager;
>>>>>>>> f7a8d76678f (Avoid writing scm meta to git and use relative paths, throw apps not found error and no changes to push error):cdap-app-fabric/src/main/java/io/cdap/cdap/internal/app/sourcecontrol/SourceControlOperationRunnerFactory.java

/**
 * Exception thrown when there's no changes needed to push to linked repository
 */
<<<<<<<< HEAD:cdap-source-control/src/main/java/io/cdap/cdap/sourcecontrol/NoChangesToPushException.java
public class NoChangesToPushException extends Exception {
  public NoChangesToPushException(String message) {
    super(message);
  }
========
public interface SourceControlOperationRunnerFactory {
  SourceControlOperationRunner create(RepositoryManager repositoryManager);
>>>>>>>> f7a8d76678f (Avoid writing scm meta to git and use relative paths, throw apps not found error and no changes to push error):cdap-app-fabric/src/main/java/io/cdap/cdap/internal/app/sourcecontrol/SourceControlOperationRunnerFactory.java
}
