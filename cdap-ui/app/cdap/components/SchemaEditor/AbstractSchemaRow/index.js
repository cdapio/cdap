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

import React, {PropTypes} from 'react';
import ArraySchemaRow from 'components/SchemaEditor/ArraySchemaRow';
import MapSchemaRow from 'components/SchemaEditor/MapSchemaRow';
import UnionSchemaRow from 'components/SchemaEditor/UnionSchemaRow';
import EnumSchemaRow from 'components/SchemaEditor/EnumSchemaRow';
import RecordSchemaRow from 'components/SchemaEditor/RecordSchemaRow';
// import {parseType} from 'components/SchemaEditor/SchemaHelpers';


require('./AbstractSchemaRow.less');

export default function AbstractSchemaRow({row, onChange}) {
  const renderSchemaRow = (row) => {
    switch(row.displayType) {
      case 'array':
        return (
          <ArraySchemaRow
            row={row.type}
            onChange={onChange}
          />
        );
      case 'map':
        return (
          <MapSchemaRow
            row={row.type}
            onChange={onChange}
          />
        );
      case 'union':
        return (
          <UnionSchemaRow
            row={row.type}
            onChange={onChange}
          />
        );
      case 'enum':
        return (
          <EnumSchemaRow
            row={row.type}
            onChange={onChange}
          />
        );
      case 'record':
        return (
          <RecordSchemaRow
            row={row.type}
            onChange={onChange}
          />
        );
      default:
        return null;
    }
  };
  return (
    <div className="abstract-schema-row">
      {
        renderSchemaRow(row)
      }
    </div>
  );
}
AbstractSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
