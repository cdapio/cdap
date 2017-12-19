/*
 * Copyright © 2017 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';

require('./StyledSelectTag.scss');

export default function StyledSelectTag({keys, onChange, defaultEmptyMessage = 'No Values'}) {
  const renderOptions = () => {
    if (Array.isArray(keys) && keys.length) {
      return keys.map(key => {
        key = key || {};
        return (
          <option key={key.id} value={key.id}>
            {key.value}
          </option>
        );
      });
     }
     return <option>{defaultEmptyMessage}</option>;
  };
  return (
    <div className="styled-select-wrapper">
      <select
        className="styled-select-tag"
        onChange={onChange ? onChange : () => {}}
      >
        { renderOptions() }
      </select>
      <IconSVG name="icon-caret-down" />
    </div>
  );
}

StyledSelectTag.propTypes = {
  keys: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    value: PropTypes.string
  })),
  onChange: PropTypes.func,
  defaultEmptyMessage: PropTypes.string
};
