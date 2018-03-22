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
import SelectWithOptions from 'components/SelectWithOptions';
import {WIDGET_PROPTYPES, DEFAULT_WIDGET_PROPS} from 'components/AbstractWidget';
require('./MemorySelectWidget.scss');

export default function MemorySelectWidget({...props}) {
  return (
    <div className="memory-select-widget">
      <SelectWithOptions
        options={props.widgetProps.options}
        value={props.value}
        className="form-control"
        onChange={props.onChange}
      />
      <span>
        GB
      </span>
    </div>
  );
}

MemorySelectWidget.propTypes = WIDGET_PROPTYPES;
MemorySelectWidget.defaultProps = DEFAULT_WIDGET_PROPS;
