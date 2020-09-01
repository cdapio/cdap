/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;

import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnable;

import java.util.concurrent.Future;

/**
 * Extends the {@link TwillController} to add extra functionalities for CDAP.
 */
public interface ExtendedTwillController extends TwillController {

  /**
   * Restart instance of a {@link TwillRunnable}.
   *
   * @param runnable the name of the runnable to restart
   * @param instanceId the instance to restart
   * @param uid an unique id to match for the restart. See also {@link ExtendedTwillContext#getUID()}
   * @return A {@link Future} that will be completed when the restart operation is completed
   */
  Future<String> restartInstance(String runnable, int instanceId, String uid);
}
