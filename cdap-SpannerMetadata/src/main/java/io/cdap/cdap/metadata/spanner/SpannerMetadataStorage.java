/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ErrorCode;



import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.metadata.Cursor;
import io.cdap.cdap.common.metadata.MetadataConflictException;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.SearchResponse;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Objects;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Arrays;


import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;



import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.cdap.cdap.spi.metadata.MetadataConstants.KEYVALUE_SEPARATOR;

/**
 * SpannerMetadataStorage implements the MetadataStorage interface
 * using Google Cloud Spanner as the underlying storage.
 * It provides methods for managing metadata within a Spanner database.
 */
public class SpannerMetadataStorage implements MetadataStorage {

    @Override
    public void initialize(MetadataStorageContext context) throws Exception {

    }

    @Override
    public void close() {
    }

    @Override
    public void createIndex() throws IOException {
    }

    @Override
    public String getName() {
        return "spanner";
    }

    @Override
    public void dropIndex() throws IOException {
    }

    @Override
    public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
       return null;
    }


    @Override
    public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
            throws IOException {
        return Collections.emptyList();
    }

    @Override
    public Metadata read(Read read) throws IOException {
        return Metadata.EMPTY;
    }
    @Override
    public SearchResponse search(SearchRequest request) throws IOException {
        return request.getCursor() != null && !request.getCursor().isEmpty()
                ? doScroll(request) : doSearch(request);
    }

    /**
     * Performs a metadata search using a provided cursor for scrolling/pagination.
     * This method parses the cursor string and then calls {@code performSpannerSearch}.
     *
     * @param request The {@link SearchRequest} containing search criteria.
     * @return A {@link SearchResponse} with results from the current cursor position.
     * @throws IOException If there is an error during the search operation.
     */
    private io.cdap.cdap.spi.metadata.SearchResponse doScroll(SearchRequest request) throws IOException {
        Cursor cursor = Cursor.fromString(request.getCursor());
        List<MetadataRecord> results = new ArrayList<>();
        return new io.cdap.cdap.spi.metadata.SearchResponse(request, cursor.toString(), request.getOffset(),
                request.getLimit(), 0, results);
    }

    /**
     * Perform a search that does continue a previous search using a cursor.
     *
     * @param request the search request
     */
    private io.cdap.cdap.spi.metadata.SearchResponse doSearch(SearchRequest request) throws IOException {
        List<MetadataRecord> results = new ArrayList<>();
        return new io.cdap.cdap.spi.metadata.SearchResponse(request, null, request.getOffset(),
                request.getLimit(), 0, results);
    }

    @Override
    public Object getDatasetMetadata(String datasetName) {
        return Collections.emptyMap();
    }

}