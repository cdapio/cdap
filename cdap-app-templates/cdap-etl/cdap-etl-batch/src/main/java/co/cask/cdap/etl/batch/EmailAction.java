/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.proto.v1.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

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
 * The user must specify a subject, the recipient's email address, and the sender's email address. Optional properties
 * are a message (the {@link WorkflowToken} for the {@link ETLBatchApplication} run will be appended to the message),
 * a host and port (defaults to localhost:25), a protocol (defaults to SMTP), and a username and password.
 * <p>
 * The action must be specified as a the only action in a list of actions in the
 * {@link co.cask.cdap.etl.batch.config.ETLBatchConfig}. It must have the name "Email"
 * or an IllegalArgumentException will be thrown.
 * </p>
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
  private Properties javaMailProperties;
  private Authenticator authenticator;

  public EmailAction(ETLStage action) {
    super(action.getPlugin().getName());
    properties = action.getPlugin().getProperties();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
    properties = ((WorkflowActionNode) context.getWorkflowSpecification().getNodeIdMap()
      .get(NAME)).getActionSpecification().getProperties();

    javaMailProperties = new Properties();
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
    if (!(Strings.isNullOrEmpty(properties.get(USERNAME)))) {
      javaMailProperties.put("mail.smtp.auth", true);
      authenticator = new Authenticator() {
        @Override
        public PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(properties.get(USERNAME), properties.get(PASSWORD));
        }
      };
    }
  }

  @Override
  public void configure() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(RECIPIENT_EMAIL_ADDRESS)),
                                String.format("You must set the \'%s\' property to send an email.",
                                              RECIPIENT_EMAIL_ADDRESS));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(FROM_ADDRESS)),
                                String.format("You must set the \'%s\' property to send an email.", FROM_ADDRESS));
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
    setName(NAME);
    setDescription("");
    setProperties(properties);
  }

  @Override
  public void run() {
    Session session = Session.getInstance(javaMailProperties, authenticator);
    session.setDebug(true);
    try {
      Message msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(properties.get(FROM_ADDRESS)));
      msg.addRecipient(Message.RecipientType.TO,
                       new InternetAddress(properties.get(RECIPIENT_EMAIL_ADDRESS)));
      msg.setSubject(properties.get(SUBJECT));
      WorkflowToken token = getContext().getToken();
      msg.setText(properties.get(MESSAGE) + "\nUSER Workflow Tokens:\n" + token.getAll(WorkflowToken.Scope.USER)
                    + "\nSYSTEM Workflow Tokens:\n" + token.getAll(WorkflowToken.Scope.SYSTEM)
      );

      String protocol = Strings.isNullOrEmpty(properties.get(PROTOCOL)) ? DEFAULT_PROTOCOL : properties.get(PROTOCOL);
      Transport transport = session.getTransport(protocol);
      transport.connect(properties.get(HOST), Integer.parseInt(properties.get(PORT)),
                        properties.get(USERNAME), properties.get(PASSWORD));
      try {
        transport.sendMessage(msg, msg.getAllRecipients());
      } finally {
        transport.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error sending email: ", e);
    }
  }
}

