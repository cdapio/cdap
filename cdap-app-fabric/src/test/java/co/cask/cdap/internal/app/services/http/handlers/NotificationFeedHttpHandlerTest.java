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
import co.cask.cdap.notifications.NotificationFeed;
import com.google.common.base.Preconditions;
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

  private static final String NAMESPACE_FIELD = "namespace";
  private static final String CATEGORY_FIELD = "category";
  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String NAMESPACE = "namespaceTest";
  private static final String CATEGORY = "categoryTest";
  private static final String NAME = "test";
  private static final String DESCRIPTION = "test description";
  private static final String ID = "namespaceTest.categoryTest.test";

  private static final NotificationFeed METADATA_VALID = new NotificationFeed.Builder().setName(NAME)
    .setNamespace(NAMESPACE).setCategory(CATEGORY).setDescription(DESCRIPTION).build();
  private static final NotificationFeed METADATA_MISSING_DESCRIPTION = new NotificationFeed.Builder().setName(NAME)
    .setNamespace(NAMESPACE).setCategory(CATEGORY).build();
  private static final NotificationFeed METADATA_EMPTY_DESCRIPTION = new NotificationFeed.Builder().setName(NAME)
    .setNamespace(NAMESPACE).setCategory(CATEGORY).setDescription("").build();
  private static final String METADATA_INVALID_JSON = "invalid";

  private HttpResponse createFeed(JsonObject jsonObject) throws Exception {
    return createFeed(GSON.toJson(jsonObject));
  }

  private HttpResponse createFeed(NotificationFeed metadata) throws Exception {
    return createFeed(GSON.toJson(metadata));
  }

  private HttpResponse createFeed(String metadata) throws Exception {
    return doPut(String.format("%s/feeds", Constants.Gateway.API_VERSION_3), metadata);
  }

  private HttpResponse listFeeds() throws Exception {
    return doGet(String.format("%s/feeds", Constants.Gateway.API_VERSION_3));
  }

  private HttpResponse getFeed(String id) throws Exception {
    Preconditions.checkArgument(id != null, "namespace name cannot be null");
    return doGet(String.format("%s/feeds/%s", Constants.Gateway.API_VERSION_3, id));
  }

  private HttpResponse deleteFeed(String id) throws Exception {
    return doDelete(String.format("%s/feeds/%s", Constants.Gateway.API_VERSION_3, id));
  }

  private void assertResponseCode(int expected, HttpResponse response) {
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  private List<JsonObject> readListResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  private JsonObject readGetResponse(HttpResponse response) throws IOException {
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  @Test
  public void testFeedsValidFlows() throws Exception {
    // no feeds initially
    HttpResponse response = listFeeds();
    assertResponseCode(200, response);
    List<JsonObject> feeds = readListResponse(response);
    Assert.assertEquals(0, feeds.size());
    try {
      // create and verify
      response = createFeed(METADATA_VALID);
      assertResponseCode(200, response);
      response = listFeeds();
      feeds = readListResponse(response);
      Assert.assertEquals(1, feeds.size());
      Assert.assertEquals(NAME, feeds.get(0).get(NAME_FIELD).getAsString());
      Assert.assertEquals(NAMESPACE, feeds.get(0).get(NAMESPACE_FIELD).getAsString());
      Assert.assertEquals(CATEGORY, feeds.get(0).get(CATEGORY_FIELD).getAsString());
      Assert.assertEquals(DESCRIPTION, feeds.get(0).get(DESCRIPTION_FIELD).getAsString());
    } finally {
      // cleanup
      response = deleteFeed(ID);
      assertResponseCode(200, response);
      response = listFeeds();
      feeds = readListResponse(response);
      Assert.assertEquals(0, feeds.size());
    }
  }

  @Test
  public void testCreateDuplicate() throws Exception {
    try {
      // prepare - create feed
      HttpResponse response = createFeed(METADATA_VALID);
      assertResponseCode(200, response);
      response = getFeed(ID);
      JsonObject feed = readGetResponse(response);
      Assert.assertNotNull(feed);
      Assert.assertEquals(NAME, feed.get(NAME_FIELD).getAsString());
      Assert.assertEquals(NAMESPACE, feed.get(NAMESPACE_FIELD).getAsString());
      Assert.assertEquals(CATEGORY, feed.get(CATEGORY_FIELD).getAsString());
      Assert.assertEquals(DESCRIPTION, feed.get(DESCRIPTION_FIELD).getAsString());

      // create again with the same name
      response = createFeed(METADATA_EMPTY_DESCRIPTION);
      assertResponseCode(409, response);
      // check that no updates happened
      response = getFeed(ID);
      feed = readGetResponse(response);
      Assert.assertNotNull(feed);
      Assert.assertEquals(NAME, feed.get(NAME_FIELD).getAsString());
      Assert.assertEquals(NAMESPACE, feed.get(NAMESPACE_FIELD).getAsString());
      Assert.assertEquals(CATEGORY, feed.get(CATEGORY_FIELD).getAsString());
      Assert.assertEquals(DESCRIPTION, feed.get(DESCRIPTION_FIELD).getAsString());
    } finally {
      // cleanup
      HttpResponse response = deleteFeed(ID);
      assertResponseCode(200, response);
    }
  }

  @Test
  public void testCreateInvalidJson() throws Exception {
    // invalid json should return 400
    HttpResponse response = createFeed(METADATA_INVALID_JSON);
    assertResponseCode(400, response);
    // verify
    response = getFeed(ID);
    assertResponseCode(404, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidName() throws Exception {
    // name must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(NAMESPACE_FIELD, GSON.toJsonTree(NAMESPACE));
    object.add(CATEGORY_FIELD, GSON.toJsonTree(CATEGORY));
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));

    HttpResponse response = createFeed(object);
    assertResponseCode(400, response);

    object.add(NAME_FIELD, GSON.toJsonTree(""));
    response = createFeed(object);
    assertResponseCode(400, response);

    object.add(NAME_FIELD, GSON.toJsonTree("$.a"));
    response = createFeed(object);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidNamespace() throws Exception {
    // namespace must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(NAME_FIELD, GSON.toJsonTree(NAME));
    object.add(CATEGORY_FIELD, GSON.toJsonTree(CATEGORY));
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));

    HttpResponse response = createFeed(object);
    assertResponseCode(400, response);

    object.add(NAMESPACE_FIELD, GSON.toJsonTree(""));
    response = createFeed(object);
    assertResponseCode(400, response);

    object.add(NAMESPACE_FIELD, GSON.toJsonTree("$.a"));
    response = createFeed(object);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingEmptyOrInvalidCategory() throws Exception {
    // category must be non-null, non-empty
    JsonObject object = new JsonObject();
    object.add(NAME_FIELD, GSON.toJsonTree(NAME));
    object.add(NAMESPACE_FIELD, GSON.toJsonTree(NAMESPACE));
    object.add(DESCRIPTION_FIELD, GSON.toJsonTree(DESCRIPTION));

    HttpResponse response = createFeed(object);
    assertResponseCode(400, response);

    object.add(CATEGORY_FIELD, GSON.toJsonTree(""));
    response = createFeed(object);
    assertResponseCode(400, response);

    object.add(CATEGORY_FIELD, GSON.toJsonTree("$.a"));
    response = createFeed(object);
    assertResponseCode(400, response);
  }

  @Test
  public void testCreateMissingOrEmptyDescription() throws Exception {
    // create with missing description
    HttpResponse response = createFeed(METADATA_MISSING_DESCRIPTION);
    assertResponseCode(200, response);
    try {
      // verify
      response = getFeed(ID);
      JsonObject feed = readGetResponse(response);
      Assert.assertNotNull(feed);
      Assert.assertEquals(NAME, feed.get(NAME_FIELD).getAsString());
      Assert.assertEquals(NAMESPACE, feed.get(NAMESPACE_FIELD).getAsString());
      Assert.assertEquals(CATEGORY, feed.get(CATEGORY_FIELD).getAsString());
      Assert.assertNull(feed.get(DESCRIPTION_FIELD));
      // cleanup
      response = deleteFeed(ID);
      assertResponseCode(200, response);

      // create with empty description
      response = createFeed(METADATA_EMPTY_DESCRIPTION);
      assertResponseCode(200, response);
      // verify
      response = getFeed(ID);
      feed = readGetResponse(response);
      Assert.assertNotNull(feed);
      Assert.assertEquals(NAME, feed.get(NAME_FIELD).getAsString());
      Assert.assertEquals(NAMESPACE, feed.get(NAMESPACE_FIELD).getAsString());
      Assert.assertEquals(CATEGORY, feed.get(CATEGORY_FIELD).getAsString());
      Assert.assertEquals("", feed.get(DESCRIPTION_FIELD).getAsString());
    } finally {
      // cleanup
      response = deleteFeed(ID);
      assertResponseCode(200, response);
    }
  }

  @Test
  public void testDeleteMissingFeed() throws Exception {
    // test deleting non-existent feed
    HttpResponse response = deleteFeed("does.not.exist");
    assertResponseCode(404, response);
    // Id has wrong format
    response = deleteFeed("doesnotexist");
    assertResponseCode(400, response);
  }
}
