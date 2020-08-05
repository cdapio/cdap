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

package io.cdap.cdap.logging.appender.tms;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.MessagingService;

/**
 * TMS Log Appender used for Preview. Only difference with {@link TMSLogAppender} is
 * an instance of MessagingService it receives.
 */
public class PreviewTMSLogAppender extends TMSLogAppender  {
  @Inject
  PreviewTMSLogAppender(CConfiguration cConf,
                        @Named("globalTMS") MessagingService messagingService) {
    super(cConf, messagingService);
  }
}
