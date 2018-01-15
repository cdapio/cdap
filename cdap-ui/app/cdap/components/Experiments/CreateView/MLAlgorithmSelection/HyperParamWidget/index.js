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

import PropTypes from 'prop-types';
import React from 'react';
import SelectWithOptions from 'components/SelectWithOptions';

require('./HyperParamWidget.scss');
const BoolWidget = ({options, value, onChange}) => {
  return (
    <div className="bool-widget">
      {options.map(option => {
        return (
          <label key={option}>
            <input
              type="radio"
              value={option}
              checked={option === value}
              className="form-control"
              onChange={onChange}
            />
            <span>{option}</span>
          </label>
        );
      })}
    </div>
  );
};
BoolWidget.propTypes = {
  options: PropTypes.arrayOf(PropTypes.string),
  value: PropTypes.string,
  onChange: PropTypes.func
};

const getComponent = (type) => {
  switch (type) {
    case 'int':
      return {
        comp: 'input',
        props: {
          type: 'number',
          step: 1,
          className: 'form-control'
        }
      };
    case 'double':
      return {
        comp: 'input',
        props: {
          type: 'number',
          step: 0.1,
          className: 'form-control'
        }
      };
    case 'bool':
      return {
        comp: BoolWidget,
        props: {}
      };
    case 'string':
      return {
        comp: SelectWithOptions,
        props: {}
      };
    default:
      return {
        comp: 'input',
        props: {
          type: 'text',
          className: 'form-control'
        }
      };
  }
};
export default function HyperParamWidget({type, config, onChange}) {
  let {comp: Component, props = {}} = getComponent(type);
  return (
    <label className="hyper-param-widget">
      <span className="param-label">{config.label || config.name || ''}</span>
      <Component {...config} {...props} onChange={onChange} />
    </label>
  );
}

HyperParamWidget.propTypes = {
  type: PropTypes.string,
  config: PropTypes.object,
  onChange: PropTypes.func
};
