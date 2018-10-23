/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
import uuidV4 from 'uuid/v4';

export default function RadioGroup({ layout, options, value }) {
  let groupName = 'radio-group-' + uuidV4();
  return (
    <div
      className={classnames('widget-radio-group', {
        'widget-radio-group-inline': layout === 'inline',
      })}
    >
      {options.map((option) => {
        return (
          <div
            className={classnames({
              radio: layout === 'block',
              'radio-inline': layout === 'inline',
            })}
          >
            <label>
              <input
                type="radio"
                id={option.id}
                value={option.id}
                name={groupName}
                checked={value === option.id}
              />
              {option.label}
            </label>
          </div>
        );
      })}
    </div>
  );
}

RadioGroup.propTypes = {
  layout: PropTypes.string,
  options: PropTypes.array,
  value: PropTypes.string,
};
