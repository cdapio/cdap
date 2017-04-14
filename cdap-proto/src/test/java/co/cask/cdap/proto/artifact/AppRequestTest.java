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
package co.cask.cdap.proto.artifact;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class AppRequestTest {
  private static final Gson GSON = new Gson();

  /**
   * Test {@link AppRequest} deserialization
   */
  @Test
  public void testAppRequestDeserialize() throws Exception {
    String appRequestWithSchedules = "{\n" +
      "  \"artifact\": {\n" +
      "     \"name\": \"cdap-notifiable-workflow\",\n" +
      "     \"version\": \"1.0.0\",\n" +
      "     \"scope\": \"system\"\n" +
      "  },\n" +
      "  \"config\": {\n" +
      "     \"plugin\": {\n" +
      "        \"name\": \"WordCount\",\n" +
      "        \"type\": \"sparkprogram\",\n" +
      "        \"artifact\": {\n" +
      "           \"name\": \"word-count-program\",\n" +
      "           \"scope\": \"user\",\n" +
      "           \"version\": \"1.0.0\"\n" +
      "        }\n" +
      "     },\n" +
      "\n" +
      "     \"notificationEmailSender\": \"sender@example.domain.com\",\n" +
      "     \"notificationEmailIds\": [\"recipient@example.domain.com\"],\n" +
      "     \"notificationEmailSubject\": \"[Critical] Workflow execution failed.\",\n" +
      "     \"notificationEmailBody\": \"Execution of Workflow running the WordCount program failed.\"\n" +
      "  },\n" +
      "  \"preview\" : {\n" +
      "    \"programName\" : \"WordCount\",\n" +
      "    \"programType\" : \"spark\"\n" +
      "    },\n" +
      "  \"principal\" : \"test2\",\n" +
      "  \"app.deploy.update.schedules\":\"false\"\n" +
      "}";
    AppRequest appRequest = GSON.fromJson(appRequestWithSchedules, AppRequest.class);
    Assert.assertNotNull(appRequest.getArtifact());
    Assert.assertEquals("cdap-notifiable-workflow", appRequest.getArtifact().getName());
    Assert.assertNotNull(appRequest.getPreview());
    Assert.assertEquals("WordCount", appRequest.getPreview().getProgramName());
    Assert.assertNotNull(appRequest.canUpdateSchedules());
    Assert.assertFalse(appRequest.canUpdateSchedules());
  }
}
