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

angular.module(PKG.name+'.commons')
.constant('SCHEMA_TYPES', {
  'types': [
    'boolean',
    'bytes',
    'date',
    'double',
    'decimal',
    'float',
    'int',
    'long',
    'string',
    'time',
    'timestamp',
    'array',
    'enum',
    'map',
    'union',
    'record'
  ],
  'simpleTypes': [
    'decimal',
    'boolean',
    'bytes',
    'date',
    'double',
    'float',
    'int',
    'long',
    'string',
    'time',
    'timestamp'
  ]
})
.factory('SchemaHelper', (avsc) => {
  function parseType(type) {
    let storedType = type;
    let nullable = false;

    if (type.getTypeName() === 'union:wrapped') {
      type = type.getTypes();

      if (type[1] && type[1].getTypeName() === 'null') {
        storedType = type[0];
        type = type[0].getTypeName();
        nullable = true;
      } else {
        type = 'union';
      }
    } else {
      type = type.getTypeName();
    }

    type = avsc.getDisplayType(type);

    return {
      displayType: type,
      type: storedType,
      nullable: nullable,
      nested: checkComplexType(type)
    };
  }

  function checkComplexType(displayType) {
    let complexTypes = ['array', 'enum', 'map', 'record', 'union'];
    return complexTypes.indexOf(displayType) !== -1 ? true : false;
  }

  return {
    parseType,
    checkComplexType
  };
});

