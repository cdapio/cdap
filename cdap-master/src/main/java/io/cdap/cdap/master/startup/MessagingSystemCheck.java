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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.MessagingServiceUtils;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup check for configurations used by the messaging system.
 */
@SuppressWarnings("unused")
class MessagingSystemCheck extends AbstractMasterCheck {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingSystemCheck.class);

  @Inject
  MessagingSystemCheck(CConfiguration cConf) {
    super(cConf);
  }

  @Override
  public void run() throws Exception {
    MessagingServiceUtils.getSystemTopics(cConf, false);
    LOG.info("Messaging System configurations verified.");
  }
}
