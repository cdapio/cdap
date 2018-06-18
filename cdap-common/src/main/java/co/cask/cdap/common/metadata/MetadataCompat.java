/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecordV2;
import com.google.common.annotations.VisibleForTesting;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Class containing helper methods to convert some of Metadata classes to older version for backward compatibility.
 */
public class MetadataCompat {

  /**
   * @return a Set of {@link MetadataRecord} which only contains the {@link MetadataRecord} which are backward
   * compatible i.e. the record do not represent a custom entity
   */
  public static Set<MetadataRecord> filterForCompatibility(Set<MetadataRecordV2> metadataRecordsV2) {
    // use linked hashset since the records are ordered and we want to maintain the ordering
    Set<MetadataRecord> metadataRecords = new LinkedHashSet<>();
    metadataRecordsV2.forEach(record -> {
      Optional<MetadataRecord> metadataRecord = MetadataCompat.makeCompatible(record);
      metadataRecord.ifPresent(metadataRecords::add);
    });
    return metadataRecords;
  }

  /**
   * @return An {@link Optional} which contains {@link MetadataRecord} if the given metadataRecordV2 is compatible i.e.
   * the record do not represent a custom entity else is absent.
   */
  @VisibleForTesting
  static Optional<MetadataRecord> makeCompatible(MetadataRecordV2 metadataRecordV2) {
    MetadataEntity metadataEntity = metadataRecordV2.getMetadataEntity();
    try {
      NamespacedEntityId entityId = EntityId.fromMetadataEntity(metadataEntity);
      return Optional.of(new MetadataRecord(entityId, metadataRecordV2.getScope(),
                                            metadataRecordV2.getProperties(), metadataRecordV2.getTags()));
    } catch (IllegalArgumentException e) {
      // ignore the custom entities for backward compatibility
      return Optional.empty();
    }
  }

  /**
   * Filters the given metadataSearchResponseV2 for backward compatibility by dropping entries for custom entity.
   *
   * @param metadataSearchResponseV2 the {@link MetadataSearchResponseV2} to be filtered
   * @return {@link MetadataSearchResponse} which does not contain records for custom entities.
   */
  public static MetadataSearchResponse makeCompatible(MetadataSearchResponseV2 metadataSearchResponseV2) {
    // use linked hashset since the search response is ordered and we want to maintain the ordering
    Set<MetadataSearchResultRecord> filteredResults = new LinkedHashSet<>();
    Set<MetadataSearchResultRecordV2> results = metadataSearchResponseV2.getResults();
    results.forEach(record -> {
      try {
        NamespacedEntityId entityId = EntityId.fromMetadataEntity(record.getMetadataEntity());
        filteredResults.add(new MetadataSearchResultRecord(entityId, record.getMetadata()));
      } catch (IllegalArgumentException e) {
        // ignore the custom entities for backward compatibility
      }
    });
    return new MetadataSearchResponse(metadataSearchResponseV2.getSort(), metadataSearchResponseV2.getOffset(),
                                      metadataSearchResponseV2.getLimit(), metadataSearchResponseV2.getNumCursors(),
                                      metadataSearchResponseV2.getTotal(), filteredResults,
                                      metadataSearchResponseV2.getCursors(), metadataSearchResponseV2.isShowHidden(),
                                      metadataSearchResponseV2.getEntityScope());
  }
}
