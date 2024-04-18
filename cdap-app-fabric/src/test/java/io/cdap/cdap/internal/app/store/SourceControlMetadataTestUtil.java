/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import io.cdap.cdap.proto.SourceControlMetadataRecord;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SourceControlMetadataTestUtil {
  public static List<SourceControlMetadataRecord> sortByNameAscending(
      List<SourceControlMetadataRecord> records) {
    return records.stream().sorted(Comparator.comparing(record -> record.getName().toLowerCase()))
        .collect(Collectors.toList());
  }

  public static  List<SourceControlMetadataRecord> sortByNameDescending(
      List<SourceControlMetadataRecord> records) {
    return records.stream().sorted(
        Comparator.comparing((SourceControlMetadataRecord record) -> record.getName().toLowerCase())
            .reversed()).collect(Collectors.toList());
  }

  public static  List<SourceControlMetadataRecord> sortByLastModifiedDescending(
      List<SourceControlMetadataRecord> records) {
    return records.stream().sorted(Comparator.nullsFirst(
            Comparator.comparing(SourceControlMetadataRecord::getLastModified,
                Comparator.nullsFirst(Comparator.naturalOrder())).reversed()))
        .collect(Collectors.toList());
  }

  public static  List<SourceControlMetadataRecord> sortByLastModifiedAscending(
      List<SourceControlMetadataRecord> records) {
    return records.stream().sorted(Comparator.nullsFirst(
        Comparator.comparing(SourceControlMetadataRecord::getLastModified,
            Comparator.nullsFirst(Comparator.naturalOrder())))).collect(Collectors.toList());
  }

  public static  List<SourceControlMetadataRecord> filterAndCollectRecords(
      List<SourceControlMetadataRecord> records, String targetNamespace) {
    return records.stream().filter(record -> record.getNamespace().equals(targetNamespace))
        .collect(Collectors.toList());
  }
}
