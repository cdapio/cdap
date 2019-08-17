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

import React from 'react';
import { objectQuery } from 'services/helpers';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import InputBase from '@material-ui/core/InputBase';

interface ITextBoxWidgetProps {
  placeholder?: string;
}

interface ITextBoxProps extends IWidgetProps<ITextBoxWidgetProps> {}

const TextBox: React.FC<ITextBoxProps> = ({ value, onChange, widgetProps, disabled }) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const placeholder = objectQuery(widgetProps, 'placeholder');
  return (
    <InputBase
      fullWidth
      value={value}
      onChange={onChangeHandler}
      placeholder={placeholder}
      readOnly={disabled}
    />
  );
};

export default TextBox;
(TextBox as any).propTypes = WIDGET_PROPTYPES;
