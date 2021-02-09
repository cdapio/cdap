/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.oauth;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Schema for OAuth store.
 */
public class OAuthStore {
  private static final String OAUTH_PROVIDER_COL = "oauthprovider";
  private static final String LOGIN_URL_COL = "loginurl";
  private static final String TOKEN_REFRESH_URL_COL = "tokenrefreshurl";
  private static final String CLIENT_CREDS_KEY_PREFIX = "oauthclientcreds";
  private static final String REFRESH_TOKEN_KEY_PREFIX = "oauthrefreshtoken";
  private static final Gson GSON = new Gson();
  private final TransactionRunner transactionRunner;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  public static final StructuredTableId TABLE_ID = new StructuredTableId("oauth");
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(OAUTH_PROVIDER_COL),
                  Fields.stringType(LOGIN_URL_COL),
                  Fields.stringType(TOKEN_REFRESH_URL_COL))
      .withPrimaryKeys(OAUTH_PROVIDER_COL)
      .build();

  public OAuthStore(
      TransactionRunner transactionRunner,
      SecureStore secureStore,
      SecureStoreManager secureStoreManager) {
    this.transactionRunner = transactionRunner;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  /**
   * Create/update an OAuth provider.
   *
   * @param oauthProvider {@link OAuthProvider} to write
   * @throws OAuthStoreException if the write fails
   */
  public void writeProvider(OAuthProvider oauthProvider) throws OAuthStoreException {
    try {
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TABLE_ID);
        table.upsert(getRow(oauthProvider));

      }, TableNotFoundException.class, InvalidFieldException.class);
    } catch (TableNotFoundException e) {
      throw new OAuthStoreException("OAuth provider table not found", e);
    } catch (InvalidFieldException e) {
      throw new OAuthStoreException("OAuth provider object fields do not match table", e);
    }

    String namespace = NamespaceId.SYSTEM.getNamespace();
    try {
      secureStoreManager.put(
          namespace,
          getClientCredsKey(oauthProvider.getName()),
          GSON.toJson(oauthProvider.getClientCredentials()),
          "OAuth client creds",
          Collections.emptyMap());
    } catch (IOException e) {
      throw new OAuthStoreException("Failed to write to OAuth provider secure storage", e);
    } catch (Exception e) {
      throw new OAuthStoreException("Namespace \"" + namespace + "\" does not exist", e);
    }
  }

  /**
   * Get an OAuth provider.
   *
   * @param name name of {@link OAuthProvider} to read
   * @throws OAuthStoreException if the read fails
   */
  public Optional<OAuthProvider> getProvider(String name) throws OAuthStoreException {
    OAuthClientCredentials clientCreds;
    try {
      String clientCredsJson = new String(
          secureStore.get(NamespaceId.SYSTEM.getNamespace(), getClientCredsKey(name)).get(),
          StandardCharsets.UTF_8);
      clientCreds = GSON.fromJson(clientCredsJson, OAuthClientCredentials.class);
    } catch (IOException e) {
      throw new OAuthStoreException("Failed to read from OAuth provider secure storage", e);
    } catch (Exception e) {
      return Optional.empty();
    }

    try {
      return TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(TABLE_ID);
        Optional<StructuredRow> row = table.read(getKey(name));
        return row.map(structuredRow -> fromRow(structuredRow, clientCreds));
      }, TableNotFoundException.class, InvalidFieldException.class);
    } catch (TableNotFoundException e) {
      throw new OAuthStoreException("OAuth provider table not found", e);
    } catch (InvalidFieldException e) {
      throw new OAuthStoreException("OAuth provider object fields do not match table", e);
    }
  }

  /**
   * Write an OAuth refresh token for the given provider and credential.
   *
   * @param oauthProvider name of OAuth provider the refresh token is sourced from
   * @param credentialId ID used to identify this credential
   * @param token the {@link OAuthRefreshToken} to write
   * @throws OAuthStoreException if the write fails
   */
  public void writeRefreshToken(String oauthProvider, String credentialId, OAuthRefreshToken token)
      throws OAuthStoreException {
    String namespace = NamespaceId.SYSTEM.getNamespace();
    try {
      secureStoreManager.put(
          namespace,
          getRefreshTokenKey(oauthProvider, credentialId),
          GSON.toJson(token),
          "OAuth refresh token",
          Collections.emptyMap());
    } catch (IOException e) {
      throw new OAuthStoreException("Failed to write OAuth refresh token", e);
    } catch (Exception e) {
      throw new OAuthStoreException("Namespace \"" + namespace + "\" does not exist", e);
    }
  }

  /**
   * Retrieve the {@link OAuthRefreshToken} associated with the given OAuth provider and credential
   *
   * @param oauthProvider name of the OAuth provider the refresh token is sourced from
   * @param credentialId ID used to identify this credential
   * @throws OAuthStoreException if the read fails
   */
  public Optional<OAuthRefreshToken> getRefreshToken(String oauthProvider, String credentialId)
      throws OAuthStoreException {
    try {
      String tokenJson = new String(
          secureStore.get(NamespaceId.SYSTEM.getNamespace(), getRefreshTokenKey(oauthProvider, credentialId)).get(),
          StandardCharsets.UTF_8);
      return Optional.of(GSON.fromJson(tokenJson, OAuthRefreshToken.class));
    } catch (IOException e) {
      throw new OAuthStoreException("Failed to read from OAuth refresh token secure storage", e);
    } catch (JsonSyntaxException e) {
      throw new OAuthStoreException("Invalid JSON for OAuth refresh token", e);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static String getClientCredsKey(String oauthProvider) {
    return String.format("%s-%s", CLIENT_CREDS_KEY_PREFIX, oauthProvider.toLowerCase());
  }

  private static String getRefreshTokenKey(String oauthProvider, String credentialId) {
    return String.format("%s-%s-%s", REFRESH_TOKEN_KEY_PREFIX, oauthProvider.toLowerCase(), credentialId.toLowerCase());
  }

  private static List<Field<?>> getKey(String name) {
    List<Field<?>> keyFields = new ArrayList<>(1);
    keyFields.add(Fields.stringField(OAUTH_PROVIDER_COL, name));
    return keyFields;
  }

  private static OAuthProvider fromRow(StructuredRow row, OAuthClientCredentials clientCreds) {
    String name = row.getString(OAUTH_PROVIDER_COL);
    String loginURL = row.getString(LOGIN_URL_COL);
    String tokenRefreshURL = row.getString(TOKEN_REFRESH_URL_COL);

    return OAuthProvider.newBuilder()
        .withName(name)
        .withLoginURL(loginURL)
        .withTokenRefreshURL(tokenRefreshURL)
        .withClientCredentials(clientCreds)
        .build();
  }

  private static List<Field<?>> getRow(OAuthProvider oauthProvider) {
    List<Field<?>> fields = new ArrayList<>(3);
    fields.add(Fields.stringField(OAUTH_PROVIDER_COL, oauthProvider.getName()));
    fields.add(Fields.stringField(LOGIN_URL_COL, oauthProvider.getLoginURL()));
    fields.add(Fields.stringField(TOKEN_REFRESH_URL_COL, oauthProvider.getTokenRefreshURL()));
    return fields;
  }
}
