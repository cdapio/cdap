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
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import InputBase from '@material-ui/core/InputBase';
import { IWidgetProps } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import withStyles from '@material-ui/core/styles/withStyles';

const CustomizedInput = withStyles(() => {
  return {
    input: {
      padding: '7px 18px 7px 12px',
      '&:focus': {
        backgroundColor: 'transparent',
      },
    },
  };
})(InputBase);

interface ISelectOptions {
  value: string | number; // We need to expand this when we have complex use cases
  label: string;
}

interface ISelectWidgetProps {
  options: ISelectOptions[] | string[] | number[];
}

interface ISelectProps extends IWidgetProps<ISelectWidgetProps> {}

const CustomSelect: React.FC<ISelectProps> = ({
  value,
  onChange,
  widgetProps,
  disabled,
  dataCy,
}: ISelectProps) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const options = objectQuery(widgetProps, 'options') || objectQuery(widgetProps, 'values') || [];
  const optionValues = options.map((opt) => {
    return ['string', 'number'].indexOf(typeof opt) !== -1 ? { value: opt, label: opt } : opt;
  });

  return (
    <Select
      fullWidth
      value={value}
      onChange={onChangeHandler}
      input={<CustomizedInput />}
      readOnly={disabled}
      inputProps={{
        'data-cy': dataCy,
      }}
    >
      {optionValues.map((opt) => (
        <MenuItem value={opt.value} key={opt.value} disabled={opt.disabled}>
          {opt.label}
        </MenuItem>
      ))}
    </Select>
  );
};

(CustomSelect as any).propTypes = WIDGET_PROPTYPES;

export default CustomSelect;
