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

angular.module(`${PKG.name}.feature.experimental`)
  .controller('ComplexSchemaController', function() {
    this.schema = '{"type":"record","name":"rec","fields":[{"name":"bool_field","type":["boolean","null"]},{"name":"int_field","type":"int"},{"name":"long_field","type":"long"},{"name":"float_field","type":"float"},{"name":"double_field","type":"double"},{"name":"bytes_field","type":"bytes"},{"name":"array_field","type":{"type":"array","items":["string","null"]}},{"name":"enum_field","type":{"type":"enum","symbols":["Monday","Tuesday","Wednesday"]}},{"name":"union_field","type":["double",{"type": "array", "items": "string" },"bytes"]},{"name":"map_field","type":{"type":"map","keys":["string","null"],"values":["int","null"]}},{"name":"record_field","type":[{"type":"record","name":"rec1","fields":[{"name":"x","type":["int","null"]},{"name":"y","type":["double","null"]}]},"null"]},{"name":"string_field","type":["string","null"]}]}';

    this.testSchema = '';

  });
