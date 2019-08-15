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
import CodeEditor from 'components/CodeEditor';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import PropTypes from 'prop-types';

interface ICodeEditorProps extends IWidgetProps<null> {
  mode: string;
  rows: number;
}

const CodeEditorWidget: React.FC<ICodeEditorProps> = ({
  value,
  onChange,
  disabled,
  mode,
  rows,
}) => {
  return (
    <CodeEditor mode={mode} rows={rows} value={value} onChange={onChange} disabled={disabled} />
  );
};

(CodeEditorWidget as any).propTypes = {
  ...WIDGET_PROPTYPES,
  mode: PropTypes.string,
  rows: PropTypes.number,
};

export default CodeEditorWidget;
