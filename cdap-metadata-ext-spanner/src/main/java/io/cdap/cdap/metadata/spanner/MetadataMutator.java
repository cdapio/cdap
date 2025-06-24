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

import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.DISCARD;
import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.GSON;
import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.METADATA_PROPS_TABLE;
import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.METADATA_TABLE;
import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.filterMetadata;
import static io.cdap.cdap.metadata.spanner.SpannerMetadataStorage.toMetadataId;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.VersionedMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Spanner mutations from metadata changes.
 */
public class MetadataMutator {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataMutator.class);

  /**
   * Creates a Spanner request that corresponds to the given mutation, along with the change
   * effected by this mutation. The request must be executed by the caller.
   *
   * @param before   the metadata for the mutation's entity before the change
   * @param mutation the mutation to apply
   * @return a Spanner request to be executed, and the change caused by the mutation.
   */
  public static ChangeRequest applyMutation(VersionedMetadata before, MetadataMutation mutation)
    throws IOException {
    LOG.trace("Applying mutation {} to entity {} with metadata {}",
              mutation, mutation.getEntity(), before.getMetadata());
    switch (mutation.getType()) {
      case CREATE:
        return create(before, (MetadataMutation.Create) mutation);
      case DROP:
        return drop(mutation.getEntity(), before);
      case UPDATE:
        return update(mutation.getEntity(), before, ((MetadataMutation.Update) mutation).getUpdates());
      case REMOVE:
        return remove(before, (MetadataMutation.Remove) mutation);
      default:
        throw new IllegalArgumentException(String.format("Unknown mutation type '%s' for %s", mutation.getType(),
                                                         mutation));
    }
  }

  /**
   * Creates the Spanner request for an entity replacement. See {@link
   * MetadataMutation.Create} for detailed semantics.
   *
   * @param before the metadata for the mutation's entity before the change.
   * @param create the mutation to apply
   * @return the list of mutation to be executed, and the change caused by the mutation
   */
  private static ChangeRequest create(VersionedMetadata before, MetadataMutation.Create create) throws IOException {
    // if the entity did not exist before, none of the directives apply and this is equivalent to update()
    if (!before.existing()) {
      return update(create.getEntity(), before, create.getMetadata());
    }

    Metadata newMetadata = create.getMetadata();
    Metadata existingMetadata = before.getMetadata();
    Map<ScopedNameOfKind, MetadataDirective> directives = create.getDirectives();
    Set<MetadataScope> affectedScopes = determineAffectedScopes(newMetadata);
    Set<ScopedName> finalTags = new HashSet<>();
    Map<ScopedName, String> finalProperties = new HashMap<>();

    // 1. Process all tags and properties in scopes NOT affected by this mutation.
    Sets.difference(MetadataScope.ALL, affectedScopes).forEach(scope -> {
      existingMetadata.getTags().stream()
        .filter(tag -> tag.getScope().equals(scope))
        .forEach(finalTags::add);
      existingMetadata.getProperties().entrySet().stream()
        .filter(entry -> entry.getKey().getScope().equals(scope))
        .forEach(entry -> finalProperties.put(entry.getKey(), entry.getValue()));
    });

    // 2. Process all directives for affected scopes.
    directives.entrySet().stream()
      .filter(entry -> affectedScopes.contains(entry.getKey().getScope()))
      .forEach(entry -> {
        ScopedNameOfKind key = entry.getKey();
        MetadataDirective directive = entry.getValue();
        ScopedName scopedName = new ScopedName(key.getScope(), key.getName());
        switch (key.getKind()) {
          case TAG:
            if (directive != MetadataDirective.PRESERVE && directive != MetadataDirective.KEEP) {
              break;
            }

            // Check if the tag existed before but was removed in the new metadata.
            boolean presentInExisting = existingMetadata.getTags().contains(scopedName);
            boolean absentInNew = !newMetadata.getTags().contains(scopedName);
            if (presentInExisting && absentInNew) {
              finalTags.add(scopedName);
            }
            break;

          case PROPERTY:
            String existingValue = existingMetadata.getProperties().get(scopedName);
            String newValue = newMetadata.getProperties().get(scopedName);

            // Skip if there was no existing value to preserve or keep.
            if (existingValue == null) {
              break;
            }

            // These variables describe the property's state change.
            boolean propertyWasChanged = !existingValue.equals(newValue);
            boolean propertyWasRemoved = newValue == null;
            if ((directive == MetadataDirective.PRESERVE && propertyWasChanged)
              || (directive == MetadataDirective.KEEP && propertyWasRemoved)) {
              finalProperties.put(scopedName, existingValue);
            }
            break;

          default:
            throw new IllegalArgumentException("Unknown or unhandled MetadataKind: " + key.getKind());
        }
      });

    Metadata after = new Metadata(finalTags, finalProperties);
    MetadataChange metadataChange = new MetadataChange(create.getEntity(), before.getMetadata(), after);
    List<Mutation> mutations = bufferWrites(create.getEntity(), before.getVersion(), after);
    return new ChangeRequest(mutations, metadataChange);
  }

  /**
   * Determines the set of scopes that this mutation applies to.
   * Scopes that do not occur in the new metadata are not changed.
   *
   * @param metadata the metadata for the mutation's entity after the change
   * @return a set of MetadataScope objects
   */
  private static Set<MetadataScope> determineAffectedScopes(Metadata metadata) {
    return Stream.concat(metadata.getTags().stream(),
                         metadata.getProperties().keySet().stream())
      .map(ScopedName::getScope)
      .collect(Collectors.toSet());
  }

  /**
   * Creates the Spanner delete request for an entity deletion. This drops the corresponding
   * metadata document from the index.
   *
   * @param before the metadata for the mutation's entity before the change
   * @return the Change Request which contains the list of mutation,
   *        and the change caused by the mutation.
   */
  private static ChangeRequest drop(MetadataEntity entity, VersionedMetadata before) {
    List<Mutation> mutations = new ArrayList<>();
    String metadataId = toMetadataId(entity);
    mutations.add(Mutation.delete(METADATA_TABLE, Key.of(metadataId)));
    return new ChangeRequest(mutations, new MetadataChange(entity, before.getMetadata(), Metadata.EMPTY));
  }

  /**
   * Creates the Spanner request for updating the metadata of an entity. This updates or
   * adds the new metadata.
   *
   * @param before the metadata for the mutation's entity before the change, and its version
   * @return the list of mutation to be executed, and the change caused by the mutation
   */
  private static ChangeRequest update(MetadataEntity entity, VersionedMetadata before, Metadata updates)
    throws IOException {
    Set<ScopedName> tags = new HashSet<>(before.getMetadata().getTags());
    tags.addAll(updates.getTags());
    Map<ScopedName, String> properties = new HashMap<>(before.getMetadata().getProperties());
    properties.putAll(updates.getProperties());
    Metadata after = new Metadata(tags, properties);
    return new ChangeRequest(bufferWrites(entity, before.getVersion(), after),
                             new MetadataChange(entity, before.getMetadata(), after));
  }

  /**
   * Creates Spanner mutations to remove specified tags and properties from an entity.
   *
   * <p>This operation does not delete the entity's row from the table. To completely
   * delete the entity, use {@link MetadataMutation.Drop}.
   *
   * @param before the metadata for the mutation's entity before the change
   * @return the list of mutations to execute and the change caused by the mutation
   */
  private static ChangeRequest remove(VersionedMetadata before, MetadataMutation.Remove remove) throws IOException {
    Metadata after = filterMetadata(before.getMetadata(), DISCARD,
                                    remove.getKinds(), remove.getScopes(), remove.getRemovals());
    return new ChangeRequest(bufferWrites(remove.getEntity(), before.getVersion(), after),
                             new MetadataChange(remove.getEntity(), before.getMetadata(), after));
  }

  /**
   * Creates Spanner Mutations for adding or updating the metadata for an entity.
   * The mutations must be executed by the caller within a transaction.
   *
   * @param entity        The metadata entity.
   * @param expectVersion The expected version for optimistic locking, or null for creation.
   * @param metadata      The metadata to write.
   * @return A list of Spanner mutations.
   */
  private static List<Mutation> bufferWrites(MetadataEntity entity, @Nullable Long expectVersion, Metadata metadata)
    throws IOException {
    List<Mutation> mutations = new ArrayList<>();
    FormattedMetadata formattedMetadata = FormattedMetadata.from(entity, metadata);
    mutations.add(createMetadataTableMutation(entity, metadata, formattedMetadata, expectVersion));
    mutations.addAll(createMetadataPropsTableMutations(entity, formattedMetadata));
    return mutations;
  }

  /**
   * Creates the Mutation for the main metadata table.
   * This method takes the pre-processed data from the SpannerMetadataBuilder.
   */
  private static Mutation createMetadataTableMutation(MetadataEntity entity, Metadata metadata,
                                                      FormattedMetadata formattedMetadata,
                                                      @Nullable Long expectVersion) {
    Mutation.WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(METADATA_TABLE)
      .set(Tables.Metadata.METADATA_ID_FIELD).to(toMetadataId(entity))
      .set(Tables.Metadata.NAMESPACE_FIELD).to(formattedMetadata.getNamespace())
      .set(Tables.Metadata.TYPE_FIELD).to(formattedMetadata.getType())
      .set(Tables.Metadata.NAME_FIELD).to(formattedMetadata.getName())
      .set(Tables.Metadata.USER_FIELD).to(formattedMetadata.getUserText())
      .set(Tables.Metadata.SYSTEM_FIELD).to(formattedMetadata.getSystemText())
      .set(Tables.Metadata.METADATA_COLUMN_FIELD).to(GSON.toJson(metadata))
      .set(Tables.Metadata.VERSION).to(expectVersion == null ? 1L : expectVersion + 1);
    formattedMetadata.getCreated().ifPresent(c -> writeBuilder
      .set(Tables.Metadata.CREATED_FIELD).to(c));
    return writeBuilder.build();
  }

  /**
   * Creates Mutations for the metadata_props table.
   * This includes a delete mutation for existing properties followed by inserts.
   */
  private static List<Mutation> createMetadataPropsTableMutations(MetadataEntity entity,
                                                                  FormattedMetadata formattedMetadata) {
    List<Mutation> propMutations = new ArrayList<>();
    String entityId = toMetadataId(entity);

    // Crucial: Delete existing properties first to ensure updates are clean.
    // This is necessary because we are doing "insert or update" for properties,
    // but existing properties that are no longer present in the new metadata
    // need to be explicitly removed.
    propMutations.add(Mutation.delete(METADATA_PROPS_TABLE, KeySet.range(KeyRange.prefix(Key.of(entityId)))));

    for (FormattedMetadata.Property prop : formattedMetadata.getMetadataProps()) {
      propMutations.add(Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
                          .set(Tables.MetadataProps.METADATA_ID_FIELD).to(entityId)
                          .set(Tables.MetadataProps.NESTED_SCOPE_FIELD).to(prop.getScope())
                          .set(Tables.MetadataProps.NESTED_NAME_FIELD).to(prop.getName())
                          .set(Tables.MetadataProps.NESTED_VALUE_FIELD).to(prop.getValue())
                          .build());
    }
    return propMutations;
  }
}

