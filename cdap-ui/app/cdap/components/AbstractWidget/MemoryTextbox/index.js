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
import {WIDGET_PROPTYPES, DEFAULT_WIDGET_PROPS} from 'components/AbstractWidget';
import {Input} from 'reactstrap';

require('./MemoryTextbox.scss');

// MemoryTextbox explicitly displays memory in GB even though it gets the value from 
// backend in MB. we are "big data". We don't talk in Mb we only start with Gb -_-
export default function MemoryTextbox({...props}) {
  let { onChange, value, widgetProps } = props;
  let min = Math.floor(widgetProps.min / 1024);
  let max = Math.floor(widgetProps.max / 1024);
  let numberValue = parseInt(value, 10);
  let size = widgetProps.size || 'large';
  value = isNaN(numberValue) ? value : numberValue;
  let memoryInGB = Math.floor(value / 1024);
  return (
    <div className={`memory-textbox-widget ${size}`}>
      <Input
        className={`number-textbox-widget ${size}`}
        type="number"
        onChange={(e) => {
          let valueInMB = Math.floor(e.target.value * 1024);
          onChange(valueInMB);
        }}
        value={memoryInGB}
        min={min}
        max={max}
      />
      <span>
        GB
      </span>
    </div>
  );
}

MemoryTextbox.propTypes = WIDGET_PROPTYPES;
MemoryTextbox.defaultProps = DEFAULT_WIDGET_PROPS;
