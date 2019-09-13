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
import classnames from 'classnames';
import { objectQuery } from 'services/helpers';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

export const styles = (theme): StyleRules => {
  return {
    inlineRadio: {
      display: 'flex',
      flexDirection: 'row',
    },
    labelControl: {
      marginBottom: 0,
    },
    errorText: {
      color: theme.palette.red[50],
    },
  };
};

interface IOption {
  id: string;
  label: string;
}

interface IRadioGroupWidgetProps {
  layout: string;
  options: IOption[];
}

interface IRadioGroupProps
  extends IWidgetProps<IRadioGroupWidgetProps>,
    WithStyles<typeof styles> {}

const RadioGroupWidgetView: React.FC<IRadioGroupProps> = ({
  widgetProps,
  classes,
  value,
  disabled,
  onChange,
  errors,
}) => {
  const options: IOption[] = objectQuery(widgetProps, 'options') || [];
  const layout = objectQuery(widgetProps, 'layout') || 'block';
  const isModelValid = options.find((option: IOption) => option.id === value);
  let error = null;

  if (!isModelValid) {
    error = `Unknown value ${value} specified.`;
  }

  function updateModel(e) {
    onChange(e.target.value);
  }

  return (
    <div>
      <If condition={error}>
        <div className="text-danger">{error}</div>
      </If>
      <RadioGroup
        className={classnames({ [classes.inlineRadio]: layout === 'inline' })}
        value={value.toString()}
        onChange={updateModel}
      >
        {options.map((option: IOption, i) => {
          let errorMsg = null;
          if (errors) {
            const errorObj = errors.find((obj) => obj.element === option.id);
            if (errorObj) {
              errorMsg = errorObj.msg;
            }
          }
          return (
            <React.Fragment>
              <FormControlLabel
                className={classes.labelControl}
                key={`${i}-${option.id}`}
                value={option.id}
                control={<Radio color="primary" />}
                label={option.label || option.id}
                disabled={disabled}
              />
              <If condition={errorMsg}>
                <div className={classes.errorText}>{errorMsg}</div>
              </If>
            </React.Fragment>
          );
        })}
      </RadioGroup>
    </div>
  );
};

const StyledRadioGroupWidget = withStyles(styles)(RadioGroupWidgetView);

export default function RadioGroupWidget(props) {
  return (
    <ThemeWrapper>
      <StyledRadioGroupWidget {...props} />
    </ThemeWrapper>
  );
}

(RadioGroupWidget as any).propTypes = WIDGET_PROPTYPES;
