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

angular.module(PKG.name + '.feature.hydrator', [])
  .constant('IMPLICIT_SCHEMA', {
    clf: '{"type":"record","name":"etlSchemaBody","fields":[{"name": "ts", "type": "long", "readonly": "true"}, {"name": "headers", "type": {"type": "map", "keys": "string", "values": "string"},"readonly":"true"},{"name":"remote_host","type":["string","null"]},{"name":"remote_login","type":["string","null"]},{"name":"auth_user","type":["string","null"]},{"name":"date","type":["string","null"]},{"name":"request","type":["string","null"]},{"name":"status","type":["int","null"]},{"name":"content_length","type":["int","null"]},{"name":"referrer","type":["string","null"]},{"name":"user_agent","type":["string","null"]}]}',

    syslog: '{"type":"record","name":"etlSchemaBody","fields":[{"name": "ts", "type": "long", "readonly": "true"}, {"name": "headers", "type": {"type": "map", "keys": "string", "values": "string"},"readonly":"true"},{"name":"timestamp","type":["string","null"]},{"name":"logsource","type":["string","null"]},{"name":"program","type":["string","null"]},{"name":"message","type":["string","null"]},{"name":"pid","type":["string","null"]}]}'
  });
