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
import InputBase from '@material-ui/core/InputBase';
import React from 'react';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface INumberWidgetProps {
  min?: number;
  max?: number;
}

interface INumberProps extends IWidgetProps<INumberWidgetProps> {}

const NumberWidget: React.FC<INumberProps> = ({
  value,
  onChange,
  disabled,
  widgetProps,
  dataCy,
}) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const min = objectQuery(widgetProps, 'min') || Number.MIN_SAFE_INTEGER;
  const max = objectQuery(widgetProps, 'max') || Number.MAX_SAFE_INTEGER;

  return (
    <InputBase
      type="number"
      fullWidth
      value={value}
      onChange={onChangeHandler}
      readOnly={disabled}
      inputProps={{
        min,
        max,
        'data-cy': dataCy,
      }}
    />
  );
};

export default NumberWidget;
(NumberWidget as any).propTypes = WIDGET_PROPTYPES;
(NumberWidget as any).getWidgetAttributes = () => {
  return {
    min: { type: 'number', required: false },
    max: { type: 'number', required: false },
    default: { type: 'number', required: false },
  };
};
