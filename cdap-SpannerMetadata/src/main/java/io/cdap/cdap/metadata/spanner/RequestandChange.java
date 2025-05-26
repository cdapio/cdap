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

import io.cdap.cdap.spi.metadata.MetadataChange;
import com.google.cloud.spanner.Mutation;

import java.util.List;

/**
 * A simple class to pass around a Spanner Mutation, along with the metadata
 * change that it effects.
 */
public class RequestandChange {

    private final Mutation mutation;
    private final MetadataChange change;

    public RequestandChange(Mutation mutation, MetadataChange change) {
        this.mutation = mutation;
        this.change = change;
    }

    public Mutation getMutation() {
        return mutation;
    }

    public MetadataChange getChange() {
        return change;
    }
}