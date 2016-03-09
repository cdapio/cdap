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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a system service.
 */
public class SystemServiceId extends EntityId {

  private final String service;

  public SystemServiceId(String service) {
    super(EntityType.SYSTEM_SERVICE);
    this.service = service;
  }

  public String getService() {
    return service;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    SystemServiceId that = (SystemServiceId) o;
    return Objects.equals(service, that.service);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), service);
  }

  @Override
  public Id toId() {
    return Id.SystemService.from(service);
  }

  @SuppressWarnings("unused")
  public static SystemServiceId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new SystemServiceId(nextAndEnd(iterator, "service"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(service);
  }

  public static SystemServiceId fromString(String string) {
    return EntityId.fromString(string, SystemServiceId.class);
  }
}
