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

package co.cask.cdap.etl.mock.alert;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.AlertPublisherContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Publishes alerts to TMS.
 */
@Plugin(type = AlertPublisher.PLUGIN_TYPE)
@Name(TMSAlertPublisher.NAME)
public class TMSAlertPublisher extends AlertPublisher {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String NAME = "TMS";
  private static final Gson GSON = new Gson();
  private final Conf conf;

  public TMSAlertPublisher(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void initialize(AlertPublisherContext context) throws Exception {
    super.initialize(context);
    try {
      context.getTopicProperties(conf.topic);
    } catch (TopicNotFoundException e) {
      context.createTopic(conf.topic);
    }
  }

  @Override
  public void publish(Iterator<Alert> alerts) throws Exception {
    MessagePublisher messagePublisher = getContext().getDirectMessagePublisher();
    while (alerts.hasNext()) {
      messagePublisher.publish(conf.topicNamespace, conf.topic, GSON.toJson(alerts.next()));
    }
  }

  /**
   * Plugin conf.
   */
  public static class Conf extends PluginConfig {
    private String topic;

    private String topicNamespace;
  }

  public static ETLPlugin getPlugin(String topic, String namespace) {
    Map<String, String> properties = new HashMap<>();
    properties.put("topic", topic);
    properties.put("topicNamespace", namespace);
    return new ETLPlugin(NAME, AlertPublisher.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("topic", new PluginPropertyField("topic", "", "string", true, false));
    properties.put("topicNamespace", new PluginPropertyField("topicNamespace", "", "string", true, false));
    return new PluginClass(AlertPublisher.PLUGIN_TYPE, NAME, "", TMSAlertPublisher.class.getName(),
                           "conf", properties);
  }
}
