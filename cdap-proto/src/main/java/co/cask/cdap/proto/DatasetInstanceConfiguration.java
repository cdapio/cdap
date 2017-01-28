/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.proto.id.KerberosPrincipalId;
import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * POJO that carries dataset type and properties information for create dataset request
 */
public final class DatasetInstanceConfiguration {
  private final String typeName;
  private final Map<String, String> properties;
  private final String description;
  @SerializedName("owner.principal")
  private final KerberosPrincipalId ownerPrincipal;

  public DatasetInstanceConfiguration(String typeName, Map<String, String> properties) {
    this(typeName, properties, null, null);
  }

  public DatasetInstanceConfiguration(String typeName, Map<String, String> properties, @Nullable String description,
                                      @Nullable KerberosPrincipalId ownerPrincipal) {
    this.typeName = typeName;
    this.properties = properties;
    this.description = description;
    this.ownerPrincipal = ownerPrincipal;
  }

  public String getTypeName() {
    return typeName;
  }

  public Map<String, String> getProperties() {
    return properties == null ? Collections.<String, String>emptyMap() : properties;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nullable
  public KerberosPrincipalId getOwnerPrincipal() {
    return ownerPrincipal;
  }
}
