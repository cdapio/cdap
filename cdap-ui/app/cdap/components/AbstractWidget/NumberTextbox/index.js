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
import {Input} from 'reactstrap';
import {WIDGET_PROPTYPES, DEFAULT_WIDGET_PROPS} from 'components/AbstractWidget';
require('./NumberTextbox.scss');

export default function NumberTextbox({...props}) {
  let { onChange, value, widgetProps } = props;
  let min = widgetProps.min || -Infinity;
  let max = widgetProps.max || Infinity;
  let numberValue = parseInt(value, 10);
  value = isNaN(numberValue) ? value : numberValue;
  return (
    <Input
      className={`number-textbox-widget`}
      type="number"
      onChange={onChange}
      value={value}
      min={min}
      max={max}
    />
  );
}
NumberTextbox.propTypes = WIDGET_PROPTYPES;
NumberTextbox.defaultProps = DEFAULT_WIDGET_PROPS;
