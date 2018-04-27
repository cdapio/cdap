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
require('./TextAreaWidget.scss');

export default function TextArea({ onChange, value, widgetProps }) {
  return (
    <Input
      type="textarea"
      onChange={onChange}
      value={value}
      {...widgetProps}
      className="text-area-widget"
    />
  );
}

TextArea.propTypes = WIDGET_PROPTYPES;
TextArea.defaultProps = DEFAULT_WIDGET_PROPS;
