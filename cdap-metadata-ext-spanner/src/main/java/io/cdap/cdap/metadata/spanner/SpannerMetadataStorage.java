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

import static java.util.Arrays.asList;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.ScopedNameTypeAdapter;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import io.cdap.cdap.spi.metadata.VersionedMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metadata storage provider that delegates to Spanner.
 */
public class SpannerMetadataStorage implements MetadataStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataStorage.class);
  static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter()).create();
  private static final Set<Mutation.Op> SUPPORTED_MUTATION_OPS =
    Collections.unmodifiableSet(new HashSet<>(asList(
      Mutation.Op.INSERT,
      Mutation.Op.UPDATE,
      Mutation.Op.INSERT_OR_UPDATE,
      Mutation.Op.DELETE
    )));

  // Metadata table names
  public static final String METADATA_TABLE = "metadata";
  public static final String METADATA_PROPS_TABLE = "metadata_props";

  private String instanceId;
  private String projectId;
  private String databaseId;

  private Spanner spanner;
  private DatabaseClient dbClient;
  private DatabaseAdminClient adminClient;

  @Override
  public void initialize(MetadataStorageContext context) throws Exception {
    Map<String, String> properties = context.getProperties();

    this.projectId = Objects.requireNonNull(properties.get("project"));
    this.instanceId = Objects.requireNonNull(properties.get("instance"));
    this.databaseId = Objects.requireNonNull(properties.get("database"));

    /*
      The Cloud Spanner Emulator is intended for local development and testing purposes only. It
      does not persist data across sessions and is not suitable for production use.
     */
    String emulatorHost = properties.get("emulator.host");
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder().setProjectId(projectId);
    if (!Strings.isNullOrEmpty(emulatorHost)) {
      optionsBuilder.setEmulatorHost(emulatorHost);
      LOG.trace("Connecting to Spanner Emulator at {}", emulatorHost);
    }
    this.spanner = optionsBuilder.build().getService();
    this.adminClient = spanner.getDatabaseAdminClient();
    this.dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

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
      "CREATE TABLE IF NOT EXISTS %s (" // metadata
        + "%s STRING(MAX) NOT NULL,"   // metadata_id
        + "%s STRING(MAX) NOT NULL," // namespace
        + "%s STRING(MAX) NOT NULL,"  // entity_type
        + "%s STRING(MAX) NOT NULL,"  // name
        + "%s INT64,"  // create_time
        + "%s STRING(MAX),"  // user
        + "%s STRING(MAX),"  // system
        + "%s JSON,"  // metadata_column
        + "%s INT64 NOT NULL,"  // version
        + "user_tokens TOKENLIST AS "  // user_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + "system_tokens TOKENLIST AS "  // system_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + "text_tokens TOKENLIST AS "  // text_tokens list
        + "(TOKENLIST_CONCAT([User_Tokens, System_Tokens])) HIDDEN,"
        + ") PRIMARY KEY (%s) ", // metadata_id
      METADATA_TABLE,
      Tables.Metadata.METADATA_ID_FIELD,
      Tables.Metadata.NAMESPACE_FIELD,
      Tables.Metadata.TYPE_FIELD,
      Tables.Metadata.NAME_FIELD,
      Tables.Metadata.CREATED_FIELD,
      Tables.Metadata.USER_FIELD,
      Tables.Metadata.SYSTEM_FIELD,
      Tables.Metadata.METADATA_COLUMN_FIELD,
      Tables.Metadata.VERSION,
      Tables.Metadata.USER_FIELD,
      Tables.Metadata.SYSTEM_FIELD,
      Tables.Metadata.METADATA_ID_FIELD
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
      "CREATE TABLE IF NOT EXISTS %s ("
        + "%s STRING(MAX) NOT NULL,"  // metadata_id
        + "%s STRING(MAX) NOT NULL," // namespace
        + "%s STRING(MAX) NOT NULL," // entity_type
        + "%s STRING(MAX) NOT NULL," // name
        + "%s STRING(MAX)," // scope
        + "%s STRING(MAX)," // value
        + "value_tokens TOKENLIST AS " // value_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + ") PRIMARY KEY (%s, %s, %s) ," // metadata_id, name, scope
        + "INTERLEAVE IN PARENT %s ON DELETE CASCADE",
      METADATA_PROPS_TABLE,
      Tables.MetadataProps.METADATA_ID_FIELD,
      Tables.MetadataProps.NAMESPACE_FIELD,
      Tables.MetadataProps.TYPE_FIELD,
      Tables.MetadataProps.NESTED_NAME_FIELD,
      Tables.MetadataProps.NESTED_SCOPE_FIELD,
      Tables.MetadataProps.NESTED_VALUE_FIELD,
      Tables.MetadataProps.NESTED_VALUE_FIELD,
      Tables.MetadataProps.METADATA_ID_FIELD,
      Tables.MetadataProps.NESTED_NAME_FIELD,
      Tables.MetadataProps.NESTED_SCOPE_FIELD,
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
  public Metadata read(Read read) throws IOException {
    try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {
      Metadata metadata = readVersionedMetadata(read.getEntity(), transaction).getMetadata();
      return filterMetadata(metadata, true, read.getKinds(),
                            read.getScopes(), read.getSelection());
    } catch (SpannerException e) {
      throw new IOException("Error reading from Spanner", e);
    }
  }

  /**
   * Applies a single metadata mutation atomically.
   *
   * @param mutation The {@link MetadataMutation} to apply.
   * @param options  {@link MutationOptions} for the operation.
   * @return The {@link MetadataChange} that occurred.
   * @throws IOException If a non-retryable Spanner error occurs.
   */
  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
    try {
      TransactionRunner runner = dbClient.readWriteTransaction();
      return runner.run(transaction -> applyAndBufferMutation(transaction, mutation));
    } catch (SpannerException e) {
      throw new IOException("Error applying mutation to Spanner for entity: " + mutation.getEntity(), e);
    }
  }

  /**
   * Processes and buffers a single metadata mutation within a Spanner transaction.
   *
   * @param transaction The active Spanner {@link TransactionContext}.
   * @param mutation    The {@link MetadataMutation} to apply.
   * @return The resulting {@link MetadataChange}.
   */
  private MetadataChange applyAndBufferMutation(TransactionContext transaction, MetadataMutation mutation)
    throws IOException {
    MetadataEntity entity = mutation.getEntity();
    VersionedMetadata before = readVersionedMetadata(entity, transaction);
    Preconditions.checkArgument(before != null,
                                "Metadata entity %s not found for mutation", entity);
    ChangeRequest intermediary = MetadataMutator.applyMutation(before, mutation);
    bufferMutations(transaction, intermediary.getMutation());
    return intermediary.getChange();
  }

  /**
   * Validates and buffers a list of mutations to be sent when the transaction commits.
   */
  private void bufferMutations(TransactionContext transaction, List<Mutation> mutations) {
    for (Mutation mutation : mutations) {
      validateMutationOperation(mutation.getOperation());
      transaction.buffer(mutation);
    }
  }

  /**
   * Validates if the given {@link Mutation.Op} is a supported operation.
   *
   * @param operation The Spanner mutation operation to validate.
   * @throws IllegalArgumentException if the operation is not one of the supported types
   *                                  (INSERT, UPDATE, INSERT_OR_UPDATE, DELETE).
   */
  private void validateMutationOperation(Mutation.Op operation) {
    if (!SUPPORTED_MUTATION_OPS.contains(operation)) {
      throw new IllegalArgumentException("Unsupported Spanner Mutation operation: " + operation);
    }
  }

  /**
   * Reads the existing metadata for an entity from the table.
   *
   * @return existing metadata along with its version in the table, or an empty metadata with null version.
   */
  @VisibleForTesting
  VersionedMetadata readVersionedMetadata(MetadataEntity entity, ReadContext transaction) throws
    RuntimeException {
    try {
      String query = String.format(
        "SELECT %s, %s FROM %s WHERE %s = @%s",
        Tables.Metadata.METADATA_COLUMN_FIELD,
        Tables.Metadata.VERSION,
        METADATA_TABLE,
        Tables.Metadata.METADATA_ID_FIELD,
        Tables.Metadata.METADATA_ID_FIELD
      );

      Statement statement = Statement.newBuilder(query)
        .bind(Tables.Metadata.METADATA_ID_FIELD).to(toMetadataId(entity))
        .build();

      ResultSet resultSet = transaction.executeQuery(statement);
      if (resultSet.next()) {
        Struct row = resultSet.getCurrentRowAsStruct();
        String metadataString = row.getJson(Tables.Metadata.METADATA_COLUMN_FIELD);
        long version = row.getLong(Tables.Metadata.VERSION);
        Metadata metadata = GSON.fromJson(metadataString, Metadata.class);
        return VersionedMetadata.of(metadata, version);
      } else {
        return VersionedMetadata.NONE;
      }
    } catch (SpannerException | JsonParseException e) {
      throw new RuntimeException("Failed to read metadata for entity " + entity + " from Spanner", e);
    }
  }

  /**
   * Applies a list of {@link MetadataMutation} operations in a single, atomic Spanner read-write transaction.
   *
   * @param mutations A list of {@link MetadataMutation} objects to apply.
   * @param options   {@link MutationOptions} for the batch operation.
   * @return A {@link List} of {@link MetadataChange} objects for all successfully processed mutations.
   * @throws IOException If a Spanner error occurs or an underlying operation fails.
   *                                         TODO (CDAP-21172): Mutation Execution can further be optimized.
   */
  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
    throws IOException {
    if (mutations.isEmpty()) {
      return Collections.emptyList();
    }

    List<MetadataChange> changes = new ArrayList<>(mutations.size());
    try {
      TransactionRunner runner = dbClient.readWriteTransaction();
      runner.run(transaction -> {
        for (MetadataMutation mutation : mutations) {
          MetadataChange change = applyAndBufferMutation(transaction, mutation);
          changes.add(change);
        }
        return null;
      });
    } catch (SpannerException e) {
      throw new IOException("Error applying batch mutations to Spanner", e);
    }
    return changes;
  }

  /**
   * Filter the metadata based on the given scopes, kinds, and selection. Based on the value of
   * {@param keep}, this can be used to keep or to discard the matching tags and properties.
   *
   * @param keep if true, only matching metadata elements are kept; otherwise only non-matching
   *             elements are kept.
   */
  public static Metadata filterMetadata(Metadata metadata, boolean keep, Set<MetadataKind> kinds,
                                  Set<MetadataScope> scopes, Set<ScopedNameOfKind> selection) {
    if (selection != null) {
      return new Metadata(
        Sets.filter(metadata.getTags(), tag -> keep == selection.contains(
          new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName()))),
        Maps.filterKeys(metadata.getProperties(), key ->
          keep == selection.contains(new ScopedNameOfKind(MetadataKind.PROPERTY, key.getScope(), key.getName())))
      );
    }
    return new Metadata(
      Sets.filter(metadata.getTags(), tag ->
        keep == (kinds.contains(MetadataKind.TAG) && scopes.contains(tag.getScope()))),
      Maps.filterKeys(metadata.getProperties(), key ->
        keep == (kinds.contains(MetadataKind.PROPERTY) && scopes.contains(key.getScope()))));
  }

  /**
   * Translate a metadata entity into a metadata_id in the table.
   */
  public static String toMetadataId(MetadataEntity entity) {
    final boolean isEntityTypeVersioned = MetadataUtil.isVersionedEntityType(entity.getType());

    String keyValuePairs = StreamSupport.stream(entity.spliterator(), false)
      .filter(kv -> !isEntityTypeVersioned || !MetadataEntity.VERSION.equalsIgnoreCase(kv.getKey()))
      .map(kv -> kv.getKey() + "=" + kv.getValue())
      .collect(Collectors.joining(","));

    return keyValuePairs.isEmpty() ? entity.getType() : entity.getType() + ":" + keyValuePairs;
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

