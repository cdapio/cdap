/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.ai.spi;

import io.cdap.cdap.proto.ApplicationDetail;

/**
 *  AI Provider interface for AI capabilities.
 */
public interface AIProvider {

     public String getName();

     default void initialize(AIProviderContext context) throws Exception {
          // no-op
     }

     /**
      * Summarizes the given application details in mentioned format.
      *
      * @param applicationDetail The detailed information about the application.
      * @param format The format in which the summary should be returned.
      * @return A summarized representation of the application details, formatted according to the specified format.
      */
     public String summarizeApp(ApplicationDetail applicationDetail, String format);
}
