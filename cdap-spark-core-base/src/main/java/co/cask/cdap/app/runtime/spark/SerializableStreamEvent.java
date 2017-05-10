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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A {@link StreamEvent} that can be serialized.
 */
public class SerializableStreamEvent extends StreamEvent implements Externalizable {

  private long timeStamp;
  private Map<String, String> headers;
  private ByteBuffer body;

  public SerializableStreamEvent() {
    // no-op. For deserialization
  }

  public SerializableStreamEvent(StreamEvent event) {
    this.timeStamp = event.getTimestamp();
    this.headers = event.getHeaders();
    this.body = event.getBody();
  }

  @Override
  public long getTimestamp() {
    return timeStamp;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public ByteBuffer getBody() {
    return body;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeLong(timeStamp);
    out.writeObject(headers);
    out.writeInt(body.remaining());
    if (body.hasArray()) {
      out.write(body.array(), body.arrayOffset() + body.position(), body.remaining());
    } else {
      out.write(Bytes.toBytes(body));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    timeStamp = in.readLong();
    headers = (Map<String, String>) in.readObject();

    int size = in.readInt();
    byte[] buffer = new byte[size];
    in.readFully(buffer);

    body = ByteBuffer.wrap(buffer);
  }
}
