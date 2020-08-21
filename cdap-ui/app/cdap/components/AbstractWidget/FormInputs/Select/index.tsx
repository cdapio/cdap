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
import Select, { SelectProps } from '@material-ui/core/Select';
import { IWidgetProps } from 'components/AbstractWidget';
import Input from '@material-ui/core/Input';
import InputBase from '@material-ui/core/InputBase';
import MenuItem from '@material-ui/core/MenuItem';
import React from 'react';
import Tooltip from '@material-ui/core/Tooltip';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { blue } from 'components/ThemeWrapper/colors';
import { isNilOrEmptyString } from 'services/helpers';
import { objectQuery } from 'services/helpers';
import withStyles from '@material-ui/core/styles/withStyles';

const CustomTooltip = withStyles((theme) => {
  return {
    tooltip: {
      backgroundColor: theme.palette.grey[200],
      color: (theme.palette as any).white[50],
      fontSize: '12px',
      wordBreak: 'break-word',
    },
  };
})(Tooltip);

const CustomizedInputBase = withStyles(() => {
  return {
    input: {
      padding: '7px 18px 7px 12px',
      '&:focus': {
        backgroundColor: 'transparent',
        outline: `1px solid ${blue[100]}`,
      },
    },
  };
})(InputBase);

const CustomizedInput = withStyles(() => {
  return {
    input: {
      padding: '7px 18px 7px 12px',
      '&:focus': {
        backgroundColor: 'transparent',
        outline: `1px solid ${blue[100]}`,
      },
    },
  };
})(Input);

const DenseMenuItem = withStyles(() => {
  return {
    root: {
      minHeight: 'unset',
      paddingTop: '3px',
      paddingBottom: '3px',
    },
  };
})(MenuItem);

const InlineSelect = withStyles(() => {
  return {
    root: {
      display: 'inline-block',
    },
  };
})(Select);

interface ISelectOptions {
  value: string | number; // We need to expand this when we have complex use cases
  label: string;
}

interface ISelectWidgetProps extends SelectProps {
  options: ISelectOptions[] | string[] | number[];
  dense?: boolean;
  inline?: boolean;
  native?: boolean;
  default?: string;
  enableUnderline?: boolean;
}

interface ISelectProps extends IWidgetProps<ISelectWidgetProps> {
  placeholder?: string;
  inputRef?: (ref: React.ReactNode) => void;
  classes?: any;
}

const CustomSelect: React.FC<ISelectProps> = ({
  value,
  onChange,
  widgetProps,
  disabled,
  dataCy,
  inputRef,
  placeholder,
  classes,
  ...restProps
}: ISelectProps) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const options = objectQuery(widgetProps, 'options') || objectQuery(widgetProps, 'values') || [];
  const dense = objectQuery(widgetProps, 'dense') || false;
  const inline = objectQuery(widgetProps, 'inline') || false;
  const native = objectQuery(widgetProps, 'native') || false;
  const MenuProps = objectQuery(widgetProps, 'MenuProps') || {};
  const enableUnderline = objectQuery(widgetProps, 'enableUnderline') || false;
  const OptionItem = native ? 'option' : dense ? DenseMenuItem : MenuItem;
  const SelectComponent = inline ? InlineSelect : Select;
  let optionValues = options.map((opt) => {
    return ['string', 'number'].indexOf(typeof opt) !== -1 ? { value: opt, label: opt } : opt;
  });
  if (!isNilOrEmptyString(placeholder)) {
    optionValues = [{ placeholder, value: '', label: placeholder }, ...optionValues];
  }

  const InputComponent = enableUnderline ? CustomizedInput : CustomizedInputBase;

  return (
    <SelectComponent
      fullWidth
      value={value}
      onChange={onChangeHandler}
      input={<InputComponent />}
      readOnly={disabled}
      inputProps={{
        'data-cy': dataCy,
      }}
      MenuProps={{
        getContentAnchorEl: null,
        anchorOrigin: {
          vertical: 'bottom',
          horizontal: 'left',
        },
        ...MenuProps,
      }}
      displayEmpty={!isNilOrEmptyString(placeholder)}
      inputRef={inputRef}
      classes={classes}
      data-cy={`select-${dataCy}`}
      {...widgetProps}
    >
      {optionValues.map((opt) => {
        const option = (
          <OptionItem
            value={opt.value}
            key={opt.value}
            disabled={opt.disabled || !isNilOrEmptyString(opt.placeholder)}
            data-cy={`option-${opt.value}`}
          >
            {opt.label}
          </OptionItem>
        );
        if (opt.tooltip) {
          return (
            <CustomTooltip title={opt.tooltip} placement={opt.tooltipPlacement || 'left'}>
              <span>{option}</span>
            </CustomTooltip>
          );
        }
        return option;
      })}
    </SelectComponent>
  );
};

(CustomSelect as any).propTypes = WIDGET_PROPTYPES;
(CustomSelect as any).getWidgetAttributes = () => {
  return {
    options: { type: 'ISelectOptions[]|string[]|number[]', required: true },
    default: { type: 'string', required: false },
    dense: { type: 'boolean', required: false },
    inline: { type: 'boolean', required: false },
    native: { type: 'boolean', required: false },
  };
};

export default CustomSelect;
