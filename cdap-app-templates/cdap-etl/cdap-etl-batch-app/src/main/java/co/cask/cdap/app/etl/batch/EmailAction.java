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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.template.etl.common.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import java.util.Map;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Sends an email to the specified email address after an ETL Batch Application run is completed.
 */
public class EmailAction extends AbstractWorkflowAction {
  private static final String NAME = "Email";
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 25;
  private static final String DEFAULT_PROTOCOL = "smtp";

  public static final String RECIPIENT_EMAIL_ADDRESS = "recipientEmailAddress";
  public static final String FROM_ADDRESS = "senderEmailAddress";
  public static final String MESSAGE = "message";
  public static final String SUBJECT = "subject";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String PROTOCOL = "protocol";
  public static final String HOST = "host";
  public static final String PORT = "port";

  private Map<String, String> properties;

  public EmailAction(ETLStage action) {
    super(action.getName());
    properties = action.getProperties();
  }

  public WorkflowActionSpecification configure() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(RECIPIENT_EMAIL_ADDRESS)),
                                String.format("You must set the \'%s\' property to send an email.",
                                              RECIPIENT_EMAIL_ADDRESS));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(FROM_ADDRESS)),
                                String.format("You must set the \'%s\' property to send an email.", FROM_ADDRESS));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(MESSAGE)),
                                String.format("You must set the \'%s\' property to send an email.", MESSAGE));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(SUBJECT)),
                                String.format("You must set the \'%s\' property to send an email.", SUBJECT));
    Preconditions.checkArgument(!(Strings.isNullOrEmpty(properties.get(USERNAME)) ^
                                  Strings.isNullOrEmpty(properties.get(PASSWORD))),
                                String.format("You must either set both username and password " +
                                                "or neither username nor password. " +
                                                "Currently, they are username: \'%s\', and password: \'%s\'",
                                              properties.get(USERNAME), properties.get(PASSWORD)));
    if (Strings.isNullOrEmpty(properties.get(HOST))) {
      properties.put(HOST, DEFAULT_HOST);
    }
    if (Strings.isNullOrEmpty(properties.get(PORT))) {
      properties.put(PORT, Integer.toString(DEFAULT_PORT));
    }
    return WorkflowActionSpecification.Builder.with().setName(NAME).setDescription("")
      .withOptions(properties).build();
  }

  @Override
  public void run() {
    properties = ((WorkflowActionNode) getContext().getWorkflowSpecification().getNodeIdMap()
      .get(NAME)).getActionSpecification().getProperties();

    Properties javaMailProperties = new Properties();
    javaMailProperties.put("mail.smtp.host", !Strings.isNullOrEmpty(properties.get(HOST)) ?
      properties.get(HOST) : DEFAULT_HOST);
    javaMailProperties.put("mail.smtp.port", !Strings.isNullOrEmpty(properties.get(PORT)) ?
      Integer.parseInt(properties.get(PORT)) : DEFAULT_PORT);
    if (!(Strings.isNullOrEmpty(properties.get(USERNAME)))) {
      javaMailProperties.put("mail.smtp.auth", true);
    }
    if ("SMTPS".equalsIgnoreCase(properties.get(PROTOCOL))) {
      javaMailProperties.put("mail.smtp.ssl.enable", true);
    }
    if ("TLS".equalsIgnoreCase(properties.get(PROTOCOL))) {
      javaMailProperties.put("mail.smtp.starttls.enable", true);
    }

    Authenticator authenticator = null;
    if (!(Strings.isNullOrEmpty(properties.get(USERNAME)))) {
      javaMailProperties.put("mail.smtp.auth", true);
      authenticator = new Authenticator() {
        @Override
        public PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(properties.get(USERNAME), properties.get(PASSWORD));
        }
      };
    }

    Session session = Session.getInstance(javaMailProperties, authenticator);
    session.setDebug(true);
    try {
      Message msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(properties.get(FROM_ADDRESS)));
      msg.addRecipient(Message.RecipientType.TO,
                       new InternetAddress(properties.get(RECIPIENT_EMAIL_ADDRESS)));
      msg.setSubject(properties.get(SUBJECT));
      msg.setText(properties.get(MESSAGE));
      Transport transport;
      if (!Strings.isNullOrEmpty(properties.get(PROTOCOL))) {
        transport = session.getTransport(properties.get(PROTOCOL));
      } else {
        transport = session.getTransport(DEFAULT_PROTOCOL);
      }
      transport.connect(properties.get(HOST), Integer.parseInt(properties.get(PORT)),
                        properties.get(USERNAME), properties.get(PASSWORD));
      transport.sendMessage(msg, msg.getAllRecipients());
    } catch (Exception e) {
      throw Throwables.propagate(new Throwable("Error sending email: ", e));
    }
  }
}

