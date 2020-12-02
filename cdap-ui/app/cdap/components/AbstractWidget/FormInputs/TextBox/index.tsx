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

import { IWidgetProps } from 'components/AbstractWidget';
import Input from '@material-ui/core/Input';
import InputBase from '@material-ui/core/InputBase';
import React from 'react';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface ITextBoxWidgetProps {
  placeholder?: string;
  default?: string;
  enableUnderline?: boolean; // TODO Add to all widgets
}

interface ITextBoxProps extends IWidgetProps<ITextBoxWidgetProps> {
  autoFocus?: boolean;
  inputRef?: (ref: React.ReactNode) => void;
  className?: string;
}

const TextBoxComponent: React.FC<ITextBoxProps> = ({
  value,
  onChange,
  onBlur,
  widgetProps,
  disabled,
  dataCy,
  autoFocus,
  inputRef,
  onKeyPress,
  className,
}) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };
  const onBlurHandler = (event: React.FocusEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onBlur === 'function') {
      onBlur(v);
    }
  };

  const placeholder = objectQuery(widgetProps, 'placeholder');
  const enableUnderline = objectQuery(widgetProps, 'enableUnderline');

  const InputComponent = enableUnderline ? Input : InputBase;

  return (
    <InputComponent
      fullWidth
      value={value}
      onChange={onChangeHandler}
      onBlur={onBlurHandler}
      onKeyPress={onKeyPress}
      placeholder={placeholder}
      readOnly={disabled}
      inputProps={{
        'data-cy': dataCy,
      }}
      autoFocus={autoFocus}
      inputRef={inputRef}
      className={className}
    />
  );
};

const TextBox = React.memo(TextBoxComponent);

(TextBox as any).propTypes = WIDGET_PROPTYPES;
(TextBox as any).getWidgetAttributes = () => {
  return {
    placeholder: { type: 'string', required: false },
    default: { type: 'string', required: false },
  };
};

export default TextBox;
