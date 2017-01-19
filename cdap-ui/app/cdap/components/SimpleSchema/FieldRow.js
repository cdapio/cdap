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
import React, { PropTypes } from 'react';
import { Input } from 'reactstrap';
import SelectWithOptions from 'components/SelectWithOptions';

require('./FieldRow.scss');
const defaultFieldTypes = [
  '',
  'boolean',
  'bytes',
  'double',
  'float',
  'int',
  'long',
  'string'
];

const FieldRow = ({className, name, type, isNullable, onKeyUp, onChange, onRemove}) => {
  return (
    <tr className={className}>
      <td>
        <Input
          value={name}
          className="input-sm"
          placeholder="Field Name"
          onKeyUp={onKeyUp.bind(null, 'name')}
          onChange={onChange.bind(null, 'name')}
        />
      </td>
      <td>
        <SelectWithOptions
          value={type}
          onChange={onChange.bind(null, 'type')}
          options={defaultFieldTypes}
          className="input-sm"
        />
      </td>
      <td>
        <input
          type="checkbox"
          checked={isNullable}
          onChange={onChange.bind(null, 'isNullable')}
        />
      </td>
      <td>
        <a
          onClick={onRemove}
          className="btn btn-sm btn-danger">
          <i className="fa fa-trash"/>
        </a>
      </td>
    </tr>
  );
};
FieldRow.propTypes = {
  className: PropTypes.string,
  name: PropTypes.string,
  type: PropTypes.string,
  isNullable: PropTypes.bool,
  onKeyUp: PropTypes.func,
  onChange: PropTypes.func,
  onRemove: PropTypes.func
};

export default FieldRow;
