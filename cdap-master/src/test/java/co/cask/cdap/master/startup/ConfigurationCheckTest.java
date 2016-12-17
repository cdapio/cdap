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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

/**
 * Tests for {@link ConfigurationCheck}
 */
public class ConfigurationCheckTest {

  private void runConfigurationCheck(String param, String invalidValue) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(param, invalidValue);
    Injector injector = Guice.createInjector(new ConfigModule(cConf));
    ConfigurationCheck configurationCheck = injector.getInstance(ConfigurationCheck.class);
    configurationCheck.run();
  }

  @Test (expected = RuntimeException.class)
  public void invalidLoggingKafkaTopicTest() {
    runConfigurationCheck(Constants.Logging.KAFKA_TOPIC, "invalid:topic");
  }

  @Test (expected = RuntimeException.class)
  public void invalidNotificationTopicTest() {
    runConfigurationCheck(Constants.Notification.TOPIC, "invalidtopic#1");
  }

  @Test (expected = RuntimeException.class)
  public void invalidAuditTopicTest() {
    runConfigurationCheck(Constants.Audit.TOPIC, "invalid*topic");
  }
}
