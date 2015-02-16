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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.authorization.SubjectIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.common.authorization.IdentifiableObject;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.UnauthorizedException;
import co.cask.common.authorization.client.AuthorizationClient;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Provides methods which provide security to netty-http handlers.
 */
public class SecureHandler {

  private final AuthorizationClient authorizationClient;
  private final boolean authorizationEnabled;
  private final ImmutableSet<String> admins;

  @Inject
  public SecureHandler(CConfiguration configuration, AuthorizationClient authorizationClient) {
    this.authorizationClient = authorizationClient;
    this.authorizationEnabled = configuration.getBoolean(Constants.Security.AUTHORIZATION_ENABLED);
    this.admins = ImmutableSet.copyOf(Splitter.on(",").split(
      Objects.firstNonNull(configuration.get(Constants.Security.ADMINS), "")));
  }

  public void sendProtectedStatus(HttpResponder responder, HttpResponseStatus responseStatus,
                                  ObjectId objectId, Iterable<Permission> requiredPermissions) {
    try {
      authorize(objectId, requiredPermissions);
      responder.sendStatus(responseStatus);
    } catch (UnauthorizedException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  public void sendProtectedString(HttpResponder responder, HttpResponseStatus responseStatus,
                                  String string,
                                  ObjectId objectId, Iterable<Permission> requiredPermissions) {
    try {
      authorize(objectId, requiredPermissions);
      responder.sendString(responseStatus, string);
    } catch (UnauthorizedException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  public void sendProtectedJson(HttpResponder responder, HttpResponseStatus responseStatus,
                                Object object, ObjectId objectId, Iterable<Permission> requiredPermissions) {
    try {
      authorize(objectId, requiredPermissions);
      responder.sendJson(responseStatus, object);
    } catch (UnauthorizedException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  public void sendProtectedByteArray(HttpResponder responder, HttpResponseStatus responseStatus,
                                     byte[] bytes, Multimap<String, String> headers,
                                     ObjectId objectId, ImmutableList<Permission> requiredPermissions) {
    try {
      authorize(objectId, requiredPermissions);
      responder.sendByteArray(responseStatus, bytes, headers);
    } catch (UnauthorizedException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    }
  }

  public <T extends IdentifiableObject> void sendProtectedJsonList(HttpResponder responder,
                                                                   HttpResponseStatus responseStatus,
                                                                   ObjectId parent,
                                                                   Iterable<T> iterable,
                                                                   Iterable<Permission> requiredPermissions) {
    Iterable<T> result = filter(iterable, requiredPermissions);
    if (Iterables.isEmpty(result) && !isAuthorized(parent, requiredPermissions)) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } else {
      responder.sendJson(responseStatus, result);
    }
  }

  public <T> void sendProtectedJsonList(HttpResponder responder, HttpResponseStatus responseStatus,
                                        ObjectId parent,
                                        Iterable<T> iterable, Iterable<Permission> requiredPermissions,
                                        Function<T, ObjectId> objectIdFunction) {

    Iterable<T> result = filter(iterable, requiredPermissions, objectIdFunction);
    if (Iterables.isEmpty(result) && !isAuthorized(parent, requiredPermissions)) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } else {
      // TODO: figure out why Iterable doesn't work in sendJson
      ImmutableList<T> body = ImmutableList.copyOf(result);
      responder.sendJson(responseStatus, body);
    }
  }

  public boolean isAuthorized(ObjectId objectId, Iterable<Permission> requiredPermissions) {
    if (!authorizationEnabled) {
      return true;
    }

    Optional<String> userId = SecurityRequestContext.getUserId();
    if (!userId.isPresent()) {
      return false;
    }

    // Handle initial admins
    if (admins.contains(userId.get())) {
      return true;
    }

    return authorizationClient.isAuthorized(objectId, ImmutableList.of(SubjectIds.user(userId.get())),
                                            requiredPermissions);
  }

  public void authorize(ObjectId objectId, Iterable<Permission> requiredPermissions) throws UnauthorizedException {
    if (!authorizationEnabled) {
      return;
    }

    Optional<String> userId = SecurityRequestContext.getUserId();
    if (!userId.isPresent()) {
      throw new UnauthorizedException("Access token is missing");
    }

    // Handle initial admins
    if (admins.contains(userId.get())) {
      return;
    }

    authorizationClient.authorize(objectId, ImmutableList.of(SubjectIds.user(userId.get())), requiredPermissions);
  }

  public <T extends IdentifiableObject> Iterable<T> filter(Iterable<T> input,
                                                           Iterable<Permission> requiredPermissions) {

    if (!authorizationEnabled) {
      return input;
    }

    Optional<String> userId = SecurityRequestContext.getUserId();
    if (!userId.isPresent()) {
      return ImmutableList.of();
    }

    // Handle initial admins
    if (admins.contains(userId.get())) {
      return input;
    }

    return authorizationClient.filter(input, ImmutableList.of(SubjectIds.user(userId.get())), requiredPermissions);
  }

  public <T> Iterable<T> filter(Iterable<T> input,
                                Iterable<Permission> requiredPermissions,
                                Function<T, ObjectId> objectIdFunction) {

    if (!authorizationEnabled) {
      return input;
    }

    Optional<String> userId = SecurityRequestContext.getUserId();
    if (!userId.isPresent()) {
      return ImmutableList.of();
    }

    // Handle initial admins
    if (admins.contains(userId.get())) {
      return input;
    }

    return authorizationClient.filter(input, ImmutableList.of(SubjectIds.user(userId.get())),
                                      requiredPermissions, objectIdFunction);
  }
}
