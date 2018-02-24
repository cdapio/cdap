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
import PropTypes from 'prop-types';

import React from 'react';
import {Input} from 'reactstrap';
import uuidV4 from 'uuid/v4';

export default function MultipleSelectWithOptions({className, value, onChange, options}) {
  return (
    <Input
      type="select"
      value={value}
      className={className}
      onChange={onChange}
      multiple
    >
      {options.map(o => {
        if (typeof o === 'object') {
          return (
            <option
              key={uuidV4()}
              value={o.id}
            >
              {o.value}
            </option>
          );
        }
        return (<option key={uuidV4()}>{o}</option>);
      })}
    </Input>
  );
}
MultipleSelectWithOptions.defaultProps = {
  value: []
};
MultipleSelectWithOptions.propTypes = {
  className: PropTypes.string,
  value: PropTypes.array,
  onChange: PropTypes.func,
  options: PropTypes.arrayOf(
    PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.shape({
        id: PropTypes.string,
        value: PropTypes.string
      })
    ])
  )
};
