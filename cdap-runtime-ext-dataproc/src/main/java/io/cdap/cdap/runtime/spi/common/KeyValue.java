/*
 * Copyright © 2020 Cask Data, Inc.
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


package io.cdap.cdap.runtime.spi.common;

/**
 * This class contains definition and method for parsing key value tags from UI.
 * @param <KEY_TYPE> tag
 *  @param <VALUE_TYPE> tag
 */

public class KeyValue<KEY_TYPE, VALUE_TYPE> {
    private final KEY_TYPE key;
    private final VALUE_TYPE value;

    /**
     *  KeyValue Oobject used for parsing data
     *
     * @param key
     * @param value
     *
     */

    public KeyValue(KEY_TYPE key, VALUE_TYPE value) {
        this.key = key;
        this.value = value;
    }

    /**
     *  Retreive Object Key
     *
     */

    public KEY_TYPE getKey() {
        return key;
    }

    /**
     *  Retreive Object Value
     */


    public VALUE_TYPE getValue() {
        return value;
    }
}
