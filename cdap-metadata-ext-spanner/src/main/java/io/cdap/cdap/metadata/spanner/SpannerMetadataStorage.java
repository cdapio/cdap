/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.metadata.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * A metadata storage provider that delegates to Spanner.
 */
public class SpannerMetadataStorage implements MetadataStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataStorage.class);

  // Metadata table names
  private static final String METADATA_TABLE = "metadata";
  private static final String METADATA_PROPS_TABLE = "metadata_props";

  public static final class MetadataTable {
    private static final String METADATA_ID_FIELD = "metadata_id";
    private static final String METADATA_COLUMN_FIELD = "metadata_column";
    private static final String NAMESPACE_FIELD = "namespace";
    private static final String TYPE_FIELD = "entity_type";
    private static final String NAME_FIELD = "name";
    private static final String VERSION = "version";
    private static final String CREATED_FIELD = "create_time";
    private static final String USER_FIELD = "user";
    private static final String SYSTEM_FIELD = "system";
  }

  public static final class MetadataPropsTable {
    private static final String METADATA_ID_FIELD = "metadata_id";
    private static final String NAMESPACE_FIELD = "namespace";
    private static final String TYPE_FIELD = "entity_type";
    private static final String NESTED_NAME_FIELD = "name";
    private static final String NESTED_SCOPE_FIELD = "scope";
    private static final String NESTED_VALUE_FIELD = "value";
  }

  private String instanceId;
  private String projectId;
  private String databaseId;

  private Spanner spanner;
  private DatabaseAdminClient adminClient;

  @Override
  public void initialize(MetadataStorageContext context) throws Exception {
    Map<String, String> properties = context.getProperties();

    this.projectId = Objects.requireNonNull(properties.get("project"));
    this.instanceId = Objects.requireNonNull(properties.get("instance"));
    this.databaseId = Objects.requireNonNull(properties.get("database"));

    this.spanner = SpannerOptions.newBuilder().setProjectId(projectId).build().getService();
    this.adminClient = spanner.getDatabaseAdminClient();

    LOG.info("SpannerMetadataStorage initialized.");
  }

  @Override
  public String getName() {
    return "gcp-spanner";
  }

  @Override
  public void createIndex() throws IOException {
    getCreateTableDDLStatement();
    LOG.info("Index creation completed.");
  }

  private void getCreateTableDDLStatement() throws IOException {
    List<String> ddlStatements = new ArrayList<>();
    ddlStatements.add(getCreateMetadataTableDDLStatement());
    ddlStatements.add(getCreateMetadataPropsTableDDLStatement());
    ddlStatements.addAll(getAllSearchIndexDDLStatements());
    executeDDLStatements(ddlStatements);
  }

  /**
   * <p>Key features of the schema:
   *     <ul>
   *         <li>**`metadata_id`:** An Unique which is used to identify the various
   *         components of the pipeline metadata.
   *         </li>
   *         <li>**`namespace`:** It contains the namespace associated with a given entity.
   *         </li>
   *         <li>**`entity_type`:** It contains the type of entity.
   *         </li>
   *         <li>**`name`:** Name of the entity.
   *         </li>
   *         <li>**`created`:** creation time of the entity.</li>
   *         <li>**`user`:** user scope related properties and tags</li>
   *         <li>**`system`:**system scope related properties and tags </li>
   *         <li>**`metadata_column`:** contains all the metadata related </li>
   *         <li>user_tokens, system_tokens and text_tokens are the Tokenlists of User,
   *         System on which we create search indexes and use them for user, system
   *         and null scope searches respectively.</li>
   *     </ul>
   * </p>
   * TODO(CDAP-21174): Add the TTl policies for cleanups.
   */
  private String getCreateMetadataTableDDLStatement() {
    return String.format(
      "CREATE TABLE IF NOT EXISTS %s (" + // metadata
        "%s STRING(MAX) NOT NULL," +  // metadata_id
        "%s STRING(MAX) NOT NULL," + // namespace
        "%s STRING(MAX) NOT NULL," + // entity_type
        "%s STRING(MAX) NOT NULL," + // name
        "%s INT64," + // create_time
        "%s STRING(MAX)," + // user
        "%s STRING(MAX)," + // system
        "%s JSON," + // metadata_column
        "%s INT64 NOT NULL," + // version
        "user_tokens TOKENLIST AS " + // user_tokens list
        "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN," +
        "system_tokens TOKENLIST AS " + // system_tokens list
        "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN," +
        "text_tokens TOKENLIST AS " + // text_tokens list
        "(TOKENLIST_CONCAT([User_Tokens, System_Tokens])) HIDDEN," +
        ") PRIMARY KEY (%s) ", // metadata_id
      METADATA_TABLE,
      MetadataTable.METADATA_ID_FIELD,
      MetadataTable.NAMESPACE_FIELD,
      MetadataTable.TYPE_FIELD,
      MetadataTable.NAME_FIELD,
      MetadataTable.CREATED_FIELD,
      MetadataTable.USER_FIELD,
      MetadataTable.SYSTEM_FIELD,
      MetadataTable.METADATA_COLUMN_FIELD,
      MetadataTable.VERSION,
      MetadataTable.USER_FIELD,
      MetadataTable.SYSTEM_FIELD,
      MetadataTable.METADATA_ID_FIELD
    );
  }

  /**
   * <p>Key features of the schema:
   *     <ul>
   *         <li>**`metadata_id`:**An Unique which is used to identify the various
   *               components of the pipeline metadata.
   *         </li>
   *         <li>**`namespace`:** It contains the namespace associated with a given entity.
   *         </li>
   *         <li>**`entity_type`:** It contains the type of entity.
   *         </li>
   *         <li>**`props_name`:** Contains the name of the properties/tags associated to an
   *             entries in metadata table.
   *         </li>
   *         <li>**`props_scope`:** Contains the scope of the properties/tags associated to an
   *             entries in metadata table.
   *         </li>
   *         <li>**`props_value`:** Contains the value of the properties/tags associated to an
   *             entries in metadata table.
   *         <li>value_tokens is the Tokenlist of Props_Value Column on which we create search
   *         indexes and use of key:value type searches.
   *         respectively.</li>
   *     </ul>
   * </p>
   */
  private String getCreateMetadataPropsTableDDLStatement() {
    return String.format(
      "CREATE TABLE IF NOT EXISTS %s (" +
        "%s STRING(MAX) NOT NULL," +  // metadata_id
        "%s STRING(MAX) NOT NULL," + // namespace
        "%s STRING(MAX) NOT NULL," + // entity_type
        "%s STRING(MAX) NOT NULL," + // name
        "%s STRING(MAX)," + // scope
        "%s STRING(MAX)," + // value
        "value_tokens TOKENLIST AS " + // value_tokens list
        "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN," +
        ") PRIMARY KEY (%s, %s, %s) ," + // metadata_id, name, scope
        "INTERLEAVE IN PARENT %s ON DELETE CASCADE",
      METADATA_PROPS_TABLE,
      MetadataPropsTable.METADATA_ID_FIELD,
      MetadataPropsTable.NAMESPACE_FIELD,
      MetadataPropsTable.TYPE_FIELD,
      MetadataPropsTable.NESTED_NAME_FIELD,
      MetadataPropsTable.NESTED_SCOPE_FIELD,
      MetadataPropsTable.NESTED_VALUE_FIELD,
      MetadataPropsTable.NESTED_VALUE_FIELD,
      MetadataPropsTable.METADATA_ID_FIELD,
      MetadataPropsTable.NESTED_NAME_FIELD,
      MetadataPropsTable.NESTED_SCOPE_FIELD,
      METADATA_TABLE
    );
  }

  private List<String> getAllSearchIndexDDLStatements() {
    List<String> ddlStatements = new ArrayList<>();

    // Create SEARCH INDEX on the Tokenized column(user_tokens) of user column.
    ddlStatements.add(String.format("CREATE SEARCH INDEX UserNgramIndex ON %s(user_tokens)", METADATA_TABLE));

    // Creates SEARCH INDEX on the Tokenized column(system_tokens) of system column.
    ddlStatements.add(String.format("CREATE SEARCH INDEX SystemNgramIndex ON %s(system_tokens)", METADATA_TABLE));

    // Creates SEARCH INDEX on the Tokenized column(text_tokens) of user and system column.
    ddlStatements.add(String.format("CREATE SEARCH INDEX TextNgramIndex ON %s(text_tokens)", METADATA_TABLE));

    // Creates SEARCH INDEX on the Tokenized column(value_tokens) of props_value column.
    ddlStatements.add(String.format("CREATE SEARCH INDEX ValueNgramIndex ON %s(value_tokens)", METADATA_PROPS_TABLE));

    return ddlStatements;
  }

  /**
   * Creates the necessary metadata tables and associated search indexes in the database.
   * This method orchestrates the creation of the core metadata table, metadata_props table
   * and search indexes to facilitate efficient querying of user,system, text, and value-based
   * data within the metadata.
   * TODO (CDAP-21176): Checking additional Schema updates for schema backward compatibility.
   */
  private void executeDDLStatements(List<String> ddlStatements) throws IOException {
    if (ddlStatements.isEmpty()) {
      LOG.debug("No ddl statements to execute");
      return;
    }
    try {
      Uninterruptibles.getUninterruptibly(
        adminClient.updateDatabaseDdl(instanceId,
                                      databaseId, ddlStatements, null));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SpannerException
        && ((SpannerException) cause).getErrorCode() == ErrorCode.FAILED_PRECONDITION) {
        LOG.debug("Concurrent statement execution error: ", e);
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void dropIndex() throws IOException {
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP TABLE IF EXISTS %s", METADATA_TABLE));
    statements.add(String.format("DROP TABLE IF EXISTS %s", METADATA_PROPS_TABLE));
    executeDDLStatements(statements);
    LOG.info("Metadata Tables dropped successfully.");
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
    throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public Metadata read(Read read) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public SearchResponse search(SearchRequest request) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public synchronized void close() {
    if (spanner != null) {
      spanner.close();
      spanner = null;
    }
  }
}

