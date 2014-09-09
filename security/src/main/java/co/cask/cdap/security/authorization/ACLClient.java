/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.http.ObjectResponse;
import com.google.common.base.Supplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * Provides ways to list and set ACLs.
 */
public interface ACLClient {

  /**
   * List ACLs by entityId
   * @param entityId entityId to list ACLs by
   * @return the ACLs belonging to the entityId
   * @throws Exception if something went wrong
   */
  public List<ACL> listACLs(EntityId entityId) throws Exception;

  /**
   * List ACLs by entityId and userId
   * @param entityId entityId to list ACLs by
   * @param userId userId to list ACLs by
   * @return the ACLs belonging to the entityId and userId
   * @throws Exception if something went wrong
   */
  public List<ACL> listACLs(EntityId entityId, String userId) throws Exception;

  /**
   * Sets an ACL for an entityId and userId
   * @param entityId entityId to set the ACL on
   * @param userId userId to set the ACL on
   * @param permissions permissions to set
   * @throws Exception if something went wrong
   */
  public void setACLForUser(EntityId entityId, String userId, List<PermissionType> permissions) throws Exception;

  /**
   * Sets an ACL for an entityId and groupId
   * @param entityId entityId to set the ACL on
   * @param groupId groupId to set the ACL on
   * @param permissions permissions to set
   * @throws Exception if something went wrong
   */
  public void setACLForGroup(EntityId entityId, String groupId, List<PermissionType> permissions) throws Exception;
}
