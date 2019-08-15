/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import PropTypes from 'prop-types';
import JSONEditor from 'components/CodeEditor/JSONEditor';

interface IJsonEditorProps extends IWidgetProps<null> {
  rows: number;
  value: string;
}

const JsonEditorWidget: React.FC<IJsonEditorProps> = ({ value, onChange, disabled, rows }) => {
  return (
    <JSONEditor mode="json" rows={rows} value={value} onChange={onChange} disabled={disabled} />
  );
};

(JsonEditorWidget as any).propTypes = {
  ...WIDGET_PROPTYPES,
  rows: PropTypes.number,
};

export default JsonEditorWidget;
