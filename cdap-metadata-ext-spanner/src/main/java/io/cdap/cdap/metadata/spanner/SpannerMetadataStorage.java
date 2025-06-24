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
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataUtil;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.ScopedNameTypeAdapter;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import io.cdap.cdap.spi.metadata.Sorting;
import io.cdap.cdap.spi.metadata.VersionedMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
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

  // Define the wildcard characters for regex matching
  private static final String SQL_WILDCARD_ANY_STRING = "*";
  private static final String SQL_WILDCARD_ANY_CHAR = "?";

  private static final Map<String, String> SORT_KEY_MAP = ImmutableMap.of(
    "entity-name", Tables.Metadata.NAME_FIELD,
    "creation-time", Tables.Metadata.CREATED_FIELD
  );

  @VisibleForTesting
  static final boolean KEEP = true;
  @VisibleForTesting
  static final boolean DISCARD = false;

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
        + "%s TOKENLIST AS "  // user_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + "%s TOKENLIST AS "  // system_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + "%s TOKENLIST AS "  // text_tokens list
        + "(TOKENLIST_CONCAT([%s, %s])) HIDDEN,"
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
      Tables.Metadata.USER_TOKEN_FIELD,
      Tables.Metadata.USER_FIELD,
      Tables.Metadata.SYSTEM_TOKEN_FIELD,
      Tables.Metadata.SYSTEM_FIELD,
      Tables.Metadata.TEXT_TOKEN_FIELD,
      Tables.Metadata.USER_TOKEN_FIELD,
      Tables.Metadata.SYSTEM_TOKEN_FIELD,
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
        + "%s STRING(MAX) NOT NULL," // name
        + "%s STRING(MAX)," // scope
        + "%s STRING(MAX)," // value
        + "%s TOKENLIST AS " // value_tokens list
        + "(TOKENIZE_SUBSTRING(%s, support_relative_search=>TRUE)) HIDDEN,"
        + ") PRIMARY KEY (%s, %s, %s) ," // metadata_id, name, scope
        + "INTERLEAVE IN PARENT %s ON DELETE CASCADE",
      METADATA_PROPS_TABLE,
      Tables.MetadataProps.METADATA_ID_FIELD,
      Tables.MetadataProps.NESTED_NAME_FIELD,
      Tables.MetadataProps.NESTED_SCOPE_FIELD,
      Tables.MetadataProps.NESTED_VALUE_FIELD,
      Tables.MetadataProps.NESTED_VALUE_TOKEN_FIELD,
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
      return filterMetadata(metadata, KEEP, read.getKinds(),
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
  public SearchResponse search(SearchRequest request)  {
    Cursor cursor = Optional.ofNullable(request.getCursor())
      .filter(s -> !s.isEmpty())
      .map(Cursor::fromString)
      .orElse(null);

    return doSearch(request, cursor);
  }

  /**
   * Executes a metadata search query against Spanner, handling query building,
   * parameter binding, and result mapping.
   *
   * @param request       The {@link SearchRequest} containing search criteria.
   * @param requestCursor An optional {@link Cursor} for pagination, providing the start point for the results.
   * @return A {@link SearchResponse} containing the search results and a cursor for the next page, if any.
   */
  private SearchResponse doSearch(SearchRequest request,
                                              @Nullable Cursor requestCursor) {
    try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {
      QueryBuildResult queryResult = buildQuery(request, requestCursor);
      String sqlTemplate = queryResult.getSql();
      Map<String, Value> params = queryResult.getParams();
      List<String> sortColumns = queryResult.getSortColumns();
      Statement.Builder statementBuilder = Statement.newBuilder(sqlTemplate);
      params.forEach((key, value) -> statementBuilder.bind(key).to(value));
      Statement statement = statementBuilder.build();

      LOG.info("Executing Spanner SQL Template: {}", statement.getSql());
      LOG.info("With Parameters: {}", statement.getParameters());

      ResultSet resultSet = transaction.executeQuery(statement);
      List<MetadataRecord> results = new ArrayList<>();
      String nextActualCursor = null;

      while (resultSet.next()) {
        results.add(mapResult(resultSet));
        nextActualCursor = createNextCursorKey(resultSet, sortColumns);
      }

      LOG.info("Found {} results.", results.size());

      return createSearchResponse(request, results, nextActualCursor);
    }
  }

  /**
   * Main query builder that orchestrates calls to helper methods.
   */
  private QueryBuildResult buildQuery(SearchRequest request, @Nullable Cursor requestCursor) {
    StringBuilder sql = new StringBuilder("SELECT * FROM metadata");
    Map<String, Value> params = new HashMap<>();
    SortDetailsResult sortDetails = getSortDetails(request);
    List<String> sortColumns = sortDetails.getColumns();
    List<Sorting.Order> sortOrders = sortDetails.getOrders();

    // Add standard filter conditions (namespaces, types, etc.) to the WHERE clause.
    List<String> conditions = appendFilterConditions(request, params);

    // Add the special WHERE condition for keyset pagination if a cursor exists.
    appendCursorCondition(requestCursor, sortColumns, sortOrders, conditions, params);
    if (!conditions.isEmpty()) {
      sql.append(" WHERE ").append(String.join(" AND ", conditions));
    }

    // Add the ORDER BY clause.
    List<String> orderByClauses = new ArrayList<>();
    for (int i = 0; i < sortColumns.size(); i++) {
      orderByClauses.add(sortColumns.get(i) + " " + sortOrders.get(i).name());
    }
    sql.append(" ORDER BY ").append(String.join(", ", orderByClauses));

    // Add the LIMIT clause with a parameter.
    sql.append(" LIMIT @limit");
    params.put("limit", Value.int64(request.getLimit()));

    return new QueryBuildResult(sql.toString(), params, sortColumns);
  }

  /**
   * Determines the final list of columns and directions for the ORDER BY clause,
   * ensuring a unique tie-breaker is always present.
   * @returns A type-safe SortDetailsResult.
   */
  private SortDetailsResult getSortDetails(SearchRequest request) {
    List<String> columns = new ArrayList<>();
    List<Sorting.Order> orders = new ArrayList<>();

    if (request.getSorting() != null) {
      columns.add(mapSortKey(request.getSorting().getKey()));
      orders.add(request.getSorting().getOrder());
    } else {
      // Default sort order if none is provided in the request
      columns.add(Tables.Metadata.NAME_FIELD);
      orders.add(Sorting.Order.ASC);
    }

    // VITAL: Add a unique tie-breaker to prevent inconsistent ordering and broken pagination.
    String primaryKey = Tables.Metadata.METADATA_ID_FIELD;
    if (!columns.contains(primaryKey)) {
      columns.add(primaryKey);
      orders.add(Sorting.Order.ASC);
    }

    return new SortDetailsResult(columns, orders);
  }

  /**
   * Appends standard WHERE conditions and their parameters for filtering the search.
   */
  private List<String> appendFilterConditions(SearchRequest request, Map<String, Value> params) {
    List<String> conditions = new ArrayList<>();

    if (request.getNamespaces() != null && !request.getNamespaces().isEmpty()) {
      conditions.add("namespace IN UNNEST(@namespaces)");
      params.put("namespaces", Value.stringArray(request.getNamespaces()));
    }

    if (request.getTypes() != null && !request.getTypes().isEmpty()) {
      conditions.add("entity_type IN UNNEST(@types)");
      params.put("types", Value.stringArray(request.getTypes()));
    }

    String query = request.getQuery().toLowerCase();
    if (query.isEmpty() || query.equals("*")) {
      return conditions;
    }

    if (query.contains(":")) {
      conditions.add(buildKeyValueSearchCondition(query, params));
    } else {
      conditions.add(buildScopedSearchCondition(query, request.getScope(), params));
    }

    return conditions;
  }

  /**
   * Builds the condition for a key:value search (e.g., "tags:retail").
   * Now uses the precise SEARCH_SUBSTRING syntax for the value part.
   *
   * @param term  The full query string, like "a:b"
   * @param params The map to add bind parameters to.
   * @return The parameterized SQL condition string.
   */
  private String buildKeyValueSearchCondition(String term, Map<String, Value> params) {
    String[] parts = term.split(MetadataConstants.KEYVALUE_SEPARATOR, 2);
    String key = parts[0].trim();
    String value = parts[1].trim();

    String keyParam = "propKey_" + params.size();
    params.put(keyParam, Value.string(key));
    if (isWildcardPattern(value)) {
      String regexPattern = convertToRegexpPattern(value);
      String valueParam = "propValueRegex_" + params.size();
      params.put(valueParam, Value.string(regexPattern));

      return String.format(
        "EXISTS (SELECT 1 FROM %s "
          + "WHERE %s = %s.%s "
          + "AND %s = @%s "
          + "AND REGEXP_CONTAINS(%s, @%s))",
        METADATA_PROPS_TABLE,
        Tables.MetadataProps.METADATA_ID_FIELD,
        METADATA_TABLE,
        Tables.Metadata.METADATA_ID_FIELD,
        Tables.MetadataProps.NESTED_NAME_FIELD,
        keyParam,
        Tables.MetadataProps.NESTED_VALUE_FIELD,
        valueParam
      );
    } else {
      String valueParam = "propValueExact_" + params.size();
      params.put(valueParam, Value.string(value));
      return String.format(
        "EXISTS (SELECT 1 FROM %s "
          + "WHERE %s = %s.%s "
          + "AND %s = @%s "
          + "AND SEARCH_SUBSTRING(%s, @%s, relative_search_type=>'word_prefix'))",
        METADATA_PROPS_TABLE,
        Tables.MetadataProps.METADATA_ID_FIELD,
        METADATA_TABLE,
        Tables.Metadata.METADATA_ID_FIELD,
        Tables.MetadataProps.NESTED_NAME_FIELD,
        keyParam,
        Tables.MetadataProps.NESTED_VALUE_TOKEN_FIELD,
        valueParam
      );
    }
  }

  /**
   * Checks if a given string contains SQL-style wildcard characters.
   *
   * @param s The string to check.
   * @return true if '*' or '?' are present, false otherwise.
   */
  private boolean isWildcardPattern(String s) {
    return s.contains(SQL_WILDCARD_ANY_STRING) || s.contains(SQL_WILDCARD_ANY_CHAR);
  }

  /**
   * Converts a search pattern containing SQL-style wildcards into a format usable by regular expression matching.
   * This means:
   * <ul>
   * <li>'*' (matches any sequence of characters) becomes '.*'</li>
   * <li>'?' (matches any single character) becomes '.'</li>
   * </ul>
   * Other special characters in the pattern are treated as literal text.
   *
   * @param sqlWildcardPattern The input pattern (e.g., "la*si?").
   * @return The converted regular expression (e.g., "la.*si.").
   */
  private String convertToRegexpPattern(String sqlWildcardPattern) {
    if (sqlWildcardPattern == null) {
      return null;
    }
    StringBuilder re2Pattern = new StringBuilder();
    for (char c : sqlWildcardPattern.toCharArray()) {
      switch (c) {
        case '*':
          re2Pattern.append(".*");
          break;
        case '?':
          re2Pattern.append(".");
          break;
        default:
          re2Pattern.append(c);
          break;
      }
    }
    return re2Pattern.toString();
  }

  /**
   * Builds the condition for a simple term search based on the provided scope.
   *
   * @param searchTerm The term to search for.
   * @param scope      The scope from the search request (USER, SYSTEM, or null).
   * @param params     The map to add bind parameters to.
   * @return The parameterized SQL condition string.
   */
  private String buildScopedSearchCondition(String searchTerm, @Nullable MetadataScope scope,
                                            Map<String, Value> params) {
    String searchColumn;

    if (scope == MetadataScope.USER) {
      searchColumn = Tables.Metadata.USER_TOKEN_FIELD;
    } else if (scope == MetadataScope.SYSTEM) {
      searchColumn = Tables.Metadata.SYSTEM_TOKEN_FIELD;
    } else {
      searchColumn = Tables.Metadata.TEXT_TOKEN_FIELD;
    }

    String termParam = "searchTerm_" + params.size();
    params.put(termParam, Value.string(searchTerm));

    return String.format(
      "SEARCH_SUBSTRING(%s, @%s, relative_search_type=>'word_prefix')",
      searchColumn,
      termParam
    );
  }

  /**
   * Appends the complex WHERE condition for keyset pagination if a cursor is provided.
   */
  private void appendCursorCondition(@Nullable Cursor requestCursor, List<String> sortColumns,
                                     List<Sorting.Order> sortOrders, List<String> conditions,
                                     Map<String, Value> params) {
    if (requestCursor == null || requestCursor.getActualCursor() == null) {
      return;
    }
    String[] cursorValues = requestCursor.getActualCursor().split(",", -1);
    if (cursorValues.length != sortColumns.size()) {
      LOG.warn("Cursor values count ({}) does not match sort columns count ({}). Ignoring cursor.",
               cursorValues.length, sortColumns.size());
      return;
    }

    // Build the OR clauses for each level of sorting declaratively.
    List<String> combinedRowConditions = IntStream.range(0, sortColumns.size())
      .mapToObj(i -> {
        // For each sort column, create a clause like: (colA = valA AND colB > valB)
        List<String> clauseParts = new ArrayList<>();

        // Add equality conditions for all preceding sort columns (the "prefix")
        IntStream.range(0, i)
          .forEach(j -> clauseParts.add(
            buildCursorSubCondition(sortColumns.get(j), "=", cursorValues[j], params)));

        // Add the boundary condition ('>' or '<') for the current sort column
        String operator = (sortOrders.get(i) == Sorting.Order.ASC) ? ">" : "<";
        clauseParts.add(
          buildCursorSubCondition(sortColumns.get(i), operator, cursorValues[i], params));

        return "(" + String.join(" AND ", clauseParts) + ")";
      })
      .collect(Collectors.toList());

    if (!combinedRowConditions.isEmpty()) {
      conditions.add("(" + String.join(" OR ", combinedRowConditions) + ")");
    }
  }

  /**
   * Helper method to create a single parameterized SQL condition for the cursor.
   * Example: "name = @cursor_param_name" or "created > @cursor_param_created"
   */
  private String buildCursorSubCondition(String column, String operator, String value, Map<String, Value> params) {
    String paramName = "cursor_param_" + column + "_" + params.size();
    params.put(paramName, Value.string(value));
    return String.format("%s %s @%s", column, operator, paramName);
  }

  /**
   * Creates the "actualCursor" part of the next cursor.
   */
  private String createNextCursorKey(ResultSet resultSet, List<String> sortColumns) {
    String[] cursorValues = new String[sortColumns.size()];

    for (int i = 0; i < sortColumns.size(); i++) {
      String columnName = sortColumns.get(i);
      if (columnName.equals("create_time")) {
        cursorValues[i] = String.valueOf(resultSet.getLong(columnName));
      } else {
        cursorValues[i] = resultSet.getString(columnName);
      }
    }
    return String.join(",", cursorValues);
  }

  /**
   * Creates the final SearchResponse, packaging the next cursor string.
   */
  private SearchResponse createSearchResponse(SearchRequest request, List<MetadataRecord> results,
                                              String nextActualCursor) {
    String finalCursorString = (nextActualCursor != null) ?
      getCursor(request, results, nextActualCursor).toString() : null;
    return new SearchResponse(request, finalCursorString, request.getOffset(),
                              request.getLimit(), results.size(), results);
  }

  /**
   * Creates a pagination cursor for the next set of search results.
   *
   * @param request          The original search request.
   * @param results          The list of metadata records returned in the current page.
   * @param nextActualCursor The database-specific cursor string for the next page.
   * @return A {@link Cursor} object representing the next page's starting point.
   */
  private Cursor getCursor(SearchRequest request, List<MetadataRecord> results, String nextActualCursor) {
    int nextOffset = request.getOffset() + results.size();
    String sortingString = Optional.ofNullable(request.getSorting())
      .map(sorting -> sorting.getKey() + MetadataConstants.KEYVALUE_SEPARATOR + sorting.getOrder().name())
      .orElse(null);

    return new Cursor(
      nextOffset,
      request.getLimit(),
      false,
      request.getScope(),
      request.getNamespaces(),
      request.getTypes(),
      sortingString,
      nextActualCursor,
      request.getQuery()
    );
  }

  /**
   * Maps a Spanner {@link ResultSet} row to a {@link MetadataRecord}.
   * Extracts the metadata entity ID and JSON metadata from the result set.
   *
   * @param resultSet The Spanner result set, positioned at the current row.
   * @return A {@link MetadataRecord} representing the current row's data.
   */
  private MetadataRecord mapResult(ResultSet resultSet) {
    String metadataId = resultSet.getString(Tables.Metadata.METADATA_ID_FIELD);
    Struct row = resultSet.getCurrentRowAsStruct();
    String metadataJson = row.getJson(7);
    Metadata metadata = GSON.fromJson(metadataJson, Metadata.class);
    MetadataEntity entity = toMetadataEntity(metadataId);
    return new MetadataRecord(entity, metadata);
  }

  /**
   * Maps a user-provided sort key string to its corresponding Spanner column name.
   *
   * @param key The user-provided sort key (e.g., "name", "namespace", "entity_type").
   * @return The Spanner database column name for the given sort key.
   * @throws IllegalArgumentException if the sort key is not supported.
   */
  private static String mapSortKey(String key) {
    String newKey = SORT_KEY_MAP.get(key);
    if (newKey != null) {
      return newKey;
    }

    throw new IllegalArgumentException("Unsupported sort key: " + key);
  }

  /**
   * Translate a metadata id in the index into a metadata entity.
   */
  private static MetadataEntity toMetadataEntity(String metadataId) {
    int index = metadataId.indexOf(':');
    if (index < 0) {
      throw new IllegalArgumentException(
        "Metadata Id must be of the form 'type:k=v,...' but is " + metadataId);
    }
    String type = metadataId.substring(0, index);
    MetadataEntity.Builder builder = MetadataEntity.builder();
    for (String part : metadataId.substring(index + 1).split(",")) {
      String[] parts = part.split("=", 2);
      if (parts[0].equals(type)) {
        builder.appendAsType(parts[0], parts[1]);
      } else {
        builder.append(parts[0], parts[1]);
      }
    }

    // if it is a versioned entity then add the default version
    return MetadataUtil.addVersionIfNeeded(builder.build());
  }


  @Override
  public synchronized void close() {
    if (spanner != null) {
      spanner.close();
      spanner = null;
    }
  }

  // Define a class to encapsulate the results of buildQuery
  static class QueryBuildResult {
    private final String sql;
    private final Map<String, Value> params;
    private final List<String> sortColumns;

    public QueryBuildResult(String sql, Map<String, Value> params, List<String> sortColumns) {
      this.sql = sql;
      this.params = params;
      this.sortColumns = sortColumns;
    }

    public String getSql() {
      return sql;
    }

    public Map<String, Value> getParams() {
      return params;
    }

    public List<String> getSortColumns() {
      return sortColumns;
    }
  }

  // Define a class to encapsulate the results of getSortDetails
  static class SortDetailsResult {
    private final List<String> columns;
    private final List<Sorting.Order> orders;

    public SortDetailsResult(List<String> columns, List<Sorting.Order> orders) {
      this.columns = columns;
      this.orders = orders;
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<Sorting.Order> getOrders() {
      return orders;
    }
  }
}

