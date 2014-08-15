/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.common.conf;

import co.cask.cdap.common.io.Codec;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public abstract class PropertyStoreTestBase {

  private static final Codec<String> STRING_CODEC = new Codec<String>() {
    @Override
    public byte[] encode(String object) throws IOException {
      return object.getBytes(Charsets.UTF_8);
    }

    @Override
    public String decode(byte[] data) throws IOException {
      return new String(data, Charsets.UTF_8);
    }
  };

  protected abstract <T> PropertyStore<T> createPropertyStore(Codec<T> codec);

  @Test
  public void testBasicStore() throws InterruptedException, IOException {
    PropertyStore<String> store = createPropertyStore(STRING_CODEC);

    // Add a listener before the property watch exists
    final BlockingQueue<String> changes = new LinkedBlockingQueue<String>();
    store.addChangeListener("basic", new AbstractPropertyChangeListener<String>() {
      @Override
      public void onChange(String name, String property) {
        changes.add(property);
      }
    });

    // Set something. The listener should see it.
    store.set("basic", "basic");
    Assert.assertEquals("basic", changes.poll(5, TimeUnit.SECONDS));

    // Add another listener. This one should receive the latest value.
    store.addChangeListener("basic", new AbstractPropertyChangeListener<String>() {
      @Override
      public void onChange(String name, String property) {
        changes.add(property);
      }
    });
    Assert.assertEquals("basic", changes.poll(5, TimeUnit.SECONDS));

    // Updates the value. Both listeners should see the changes
    store.update("basic", new SyncPropertyUpdater<String>() {
      @Override
      protected String compute(@Nullable String property) {
        if ("basic".equals(property)) {
          return property + "." + property;
        }
        return null;
      }
    });
    Assert.assertEquals("basic.basic", changes.poll(5, TimeUnit.SECONDS));
    Assert.assertEquals("basic.basic", changes.poll(5, TimeUnit.SECONDS));

    // No more events. The queue should be empty now.
    Assert.assertNull(changes.poll(2, TimeUnit.SECONDS));

    store.close();
  }
}
