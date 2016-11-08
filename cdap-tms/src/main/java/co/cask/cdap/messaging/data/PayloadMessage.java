/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.data;

/**
 *
 */
public class PayloadMessage {
  private final PayloadId id;
  private final byte[] payload;

  public PayloadMessage(PayloadId id, byte[] payload) {
    this.id = id;
    this.payload = payload;
  }

  public PayloadId getId() {
    return id;
  }

  public byte[] getPayload() {
    return payload;
  }
}
