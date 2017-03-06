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

package co.cask.cdap.notifyableworkflow;

import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Notify-able Workflow.
 */
public class NotifyableWorkflow extends AbstractWorkflow {
  private static final Logger LOG = LoggerFactory.getLogger(NotifyableWorkflow.class);
  private static final String NOTIFICATION_EMAIL_IDS = "notification.email.ids";
  private static final String NOTIFICATION_EMAIL_SUBJECT = "notification.email.subject";
  private static final String NOTIFICATION_EMAIL_BODY = "notification.email.body";

  private final Set<String> notificationEmailIds;
  private final String notificationEmailSubject;
  private final String notificationEmailBody;
  
  public NotifyableWorkflow(Set<String> notificationEmailIds, String notificationEmailSubject,
                            String notificationEmailBody) {
    this.notificationEmailIds = notificationEmailIds;
    this.notificationEmailSubject = notificationEmailSubject;
    this.notificationEmailBody = notificationEmailBody;
  }

  @Override
  protected void configure() {
    addSpark("ClassicSpark");
    Map<String, String> properties = new HashMap<>();
    properties.put(NOTIFICATION_EMAIL_IDS, Joiner.on(",").join(notificationEmailIds));
    properties.put(NOTIFICATION_EMAIL_SUBJECT, notificationEmailSubject);
    properties.put(NOTIFICATION_EMAIL_BODY, notificationEmailBody);
    setProperties(properties);
  }

  @Override
  public void destroy() {
    WorkflowContext context = getContext();
    Map<String, String> properties = context.getSpecification().getProperties();

    String notificationEmailIds = properties.get(NOTIFICATION_EMAIL_IDS);
    if (context.getRuntimeArguments().containsKey(NOTIFICATION_EMAIL_IDS)) {
      notificationEmailIds = context.getRuntimeArguments().get(NOTIFICATION_EMAIL_IDS);
    }

    String notificationEmailSubject = properties.get(NOTIFICATION_EMAIL_SUBJECT);
    if (context.getRuntimeArguments().containsKey(NOTIFICATION_EMAIL_SUBJECT)) {
      notificationEmailSubject = context.getRuntimeArguments().get(NOTIFICATION_EMAIL_SUBJECT);
    }

    String notificationEmailBody = properties.get(NOTIFICATION_EMAIL_BODY);
    if (context.getRuntimeArguments().containsKey(NOTIFICATION_EMAIL_BODY)) {
      notificationEmailBody = context.getRuntimeArguments().get(NOTIFICATION_EMAIL_BODY);
    }

    LOG.info("Notification email Ids: {}.", notificationEmailIds);
    LOG.info("Notification email subject: {}.", notificationEmailSubject);
    LOG.info("Notification email body: {}.", notificationEmailBody);

    for (Map.Entry<String, WorkflowNodeState> nodeState : context.getNodeStates().entrySet()) {
      LOG.info("Node: {}", nodeState.getKey());
      LOG.info("Status: {}", nodeState.getValue().getNodeStatus());
      LOG.info("RunId: {}", nodeState.getValue().getRunId());
      LOG.info("Failure cause: {}", nodeState.getValue().getFailureCause() == null ? null
        : nodeState.getValue().getFailureCause().getStackTrace());
    }
  }
}
