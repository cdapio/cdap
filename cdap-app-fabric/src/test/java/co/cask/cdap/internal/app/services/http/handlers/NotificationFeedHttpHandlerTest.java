/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.NotificationFeedHttpHandler}.
 */
public class NotificationFeedHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String DESCRIPTION_FIELD = "description";

  // TODO when [CDAP-903] is done, those tests will fail because the following namespace will not have been created
  // Modify the tests at that time to create the namespace, and test the behavior when using non-existant namespaces
  private static final String NAMESPACE = "namespaceTest";
  private static final String CATEGORY = "categoryTest";
  private static final String NAME = "test";
  private static final String DESCRIPTION = "test description";

  private static final NotificationFeedInfo FEED_VALID =
    new NotificationFeedInfo(NAMESPACE, CATEGORY, NAME, DESCRIPTION);
  private static final NotificationFeedInfo FEED_MISSING_DESCRIPTION =
    new NotificationFeedInfo(NAMESPACE, CATEGORY, NAME, null);
  private static final NotificationFeedInfo FEED_EMPTY_DESCRIPTION =
    new NotificationFeedInfo(NAMESPACE, CATEGORY, NAME, "");
  private static final String METADATA_INVALID_JSON = "invalid";

  private HttpResponse createFeed(NotificationFeedInfo feed) throws Exception {
    return createFeed(feed.getNamespace(), feed.getCategory(), feed.getFeed(), GSON.toJson(feed));
  }

  private HttpResponse createFeed(String namespace, String category, String name, String metadata) throws Exception {
    return doPut(String.format("%s/namespaces/%s/feeds/categories/%s/names/%s", Constants.Gateway.API_VERSION_3,
                               namespace, category, name), metadata);
  }

  private HttpResponse listFeeds(String namespaceId) throws Exception {
    return doGet(String.format("%s/namespaces/%s/feeds", Constants.Gateway.API_VERSION_3, namespaceId));
  }

  private HttpResponse getFeed(String namespaceId, String category, String name) throws Exception {
    return doGet(String.format("%s/namespaces/%s/feeds/categories/%s/names/%s", Constants.Gateway.API_VERSION_3,
                               namespaceId, category, name));
  }

  private HttpResponse deleteFeed(String namespaceId, String category, String name) throws Exception {
    return doDelete(String.format("%s/namespaces/%s/feeds/categories/%s/names/%s", Constants.Gateway.API_VERSION_3,
                                  namespaceId, category, name));
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  private List<NotificationFeedInfo> readListResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<List<NotificationFeedInfo>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private NotificationFeedInfo readGetResponse(HttpResponse response) throws IOException {
    return readResponse(response, NotificationFeedInfo.class);
  }

  @Test
  public void testFeedsValidFlows() throws Exception {
    // no feeds initially
    HttpResponse response = listFeeds(NAMESPACE);
    assertResponseCode(200, response);
    List<NotificationFeedInfo> feeds = readListResponse(response);
    Assert.assertEquals(0, feeds.size());
    try {
      // create and verify
      response = createFeed(FEED_VALID);
      assertResponseCode(200, response);
      response = listFeeds(NAMESPACE);
      feeds = readListResponse(response);
      Assert.assertEquals(1, feeds.size());
      Assert.assertEquals(FEED_VALID, feeds.get(0));
    } finally {
      // cleanup
      response = deleteFeed(NAMESPACE, CATEGORY, NAME);
      assertResponseCode(200, response);
      response = listFeeds(NAMESPACE);
      feeds = readListResponse(response);
      Assert.assertEquals(0, feeds.size());
    }
  }

  @Test
  public void testCreateDuplicate() throws Exception {
    try {
      // prepare - create feed
      HttpResponse response = createFeed(FEED_VALID);
      assertResponseCode(200, response);
      response = getFeed(NAMESPACE, CATEGORY, NAME);
      NotificationFeedInfo feed = readGetResponse(response);
      Assert.assertEquals(FEED_VALID, feed);

      // create again with the same name
      response = createFeed(FEED_EMPTY_DESCRIPTION);
      assertResponseCode(200, response);
      // check that no updates happened
      response = getFeed(NAMESPACE, CATEGORY, NAME);
      feed = readGetResponse(response);
      Assert.assertEquals(FEED_EMPTY_DESCRIPTION, feed);
    } finally {
      // cleanup
      HttpResponse response = deleteFeed(NAMESPACE, CATEGORY, NAME);
      assertResponseCode(200, response);
    }
  }

  @Test
  public void testCreateInvalidJson() throws Exception {
    // invalid json should return 400
    HttpResponse response = createFeed(NAMESPACE, CATEGORY, NAME, METADATA_INVALID_JSON);
    assertResponseCode(400, response);
    // verify
    response = getFeed(NAMESPACE, CATEGORY, NAME);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidName() throws Exception {
    // name must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));
    String metadata = GSON.toJson(object);

    HttpResponse response = createFeed(NAMESPACE, CATEGORY, "", metadata);
    assertResponseCode(404, response);

    response = createFeed(NAMESPACE, CATEGORY, "$.a", metadata);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidNamespace() throws Exception {
    // namespace must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));
    String metadata = GSON.toJson(object);

    HttpResponse response = createFeed("", CATEGORY, NAME, metadata);
    assertResponseCode(404, response);

    response = createFeed("$.a", CATEGORY, NAME, metadata);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidCategory() throws Exception {
    // category must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));
    String metadata = GSON.toJson(object);

    HttpResponse response = createFeed(NAMESPACE, "", NAME, metadata);
    assertResponseCode(404, response);

    response = createFeed(NAMESPACE, "$.a", NAME, metadata);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingOrEmptyDescription() throws Exception {
    // create with missing description
    HttpResponse response = createFeed(FEED_MISSING_DESCRIPTION);
    assertResponseCode(200, response);
    try {
      // verify
      response = getFeed(NAMESPACE, CATEGORY, NAME);
      NotificationFeedInfo feed = readGetResponse(response);
      Assert.assertEquals(FEED_MISSING_DESCRIPTION, feed);
      // cleanup
      response = deleteFeed(NAMESPACE, CATEGORY, NAME);
      assertResponseCode(200, response);

      // create with empty description
      response = createFeed(FEED_EMPTY_DESCRIPTION);
      assertResponseCode(200, response);
      // verify
      response = getFeed(NAMESPACE, CATEGORY, NAME);
      feed = readGetResponse(response);
      Assert.assertEquals(FEED_EMPTY_DESCRIPTION, feed);
    } finally {
      // cleanup
      response = deleteFeed(NAMESPACE, CATEGORY, NAME);
      assertResponseCode(200, response);
    }

    response = createFeed(NAMESPACE, CATEGORY, NAME, "");
    assertResponseCode(200, response);
  }

  @Test
  public void testDeleteMissingFeed() throws Exception {
    // test deleting non-existent feed
    HttpResponse response = deleteFeed("does", "not", "exist");
    assertResponseCode(404, response);
  }
}
