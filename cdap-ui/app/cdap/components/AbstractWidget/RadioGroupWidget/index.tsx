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
import PropTypes from 'prop-types';
import classnames from 'classnames';
import { objectQuery } from 'services/helpers';

import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';

import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';

export const styles = (): StyleRules => {
  return {
    inlineRadio: {
      display: 'flex',
      flexDirection: 'row',
    },
    labelControl: {
      marginBottom: 0,
    },
  };
};
interface IWidgetAttributes {
  layout: string;
  options: IOption[];
  default: string;
}
interface IOption {
  id: string;
  label: string;
}
interface IRadioGroupWidgetProps extends WithStyles<typeof styles> {
  value: string;
  widgetAttributes: IWidgetAttributes;
  propertyName: string;
  onChange: (value: string) => void;
}

const RadioGroupWidgetView: React.FC<IRadioGroupWidgetProps> = ({
  widgetAttributes,
  propertyName,
  classes,
  value,
  onChange,
}) => {
  const options: IOption[] = objectQuery(widgetAttributes, 'options') || [];
  const layout = objectQuery(widgetAttributes, 'layout');
  const defaultValue = objectQuery(widgetAttributes, 'default') || '';
  const model = value || defaultValue;
  const isModelValid = options.find((option: IOption) => option.id === model);
  let error = null;

  if (!Array.isArray(options) || (Array.isArray(options) && !options.length)) {
    error = `Missing options for ${propertyName}`;
  }

  if (!isModelValid) {
    error = `Unknown value for ${propertyName} specified.`;
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
        value={model}
        onChange={updateModel}
      >
        {options.map((option: IOption, i) => {
          return (
            <FormControlLabel
              className={classes.labelControl}
              key={`${i}-${option.id}`}
              value={option.id}
              control={<Radio color="primary" />}
              label={option.label || option.id}
            />
          );
        })}
      </RadioGroup>
    </div>
  );
};

const RadioGroupWidget = withStyles(styles)(RadioGroupWidgetView);

export default function StyledRadioGroupWidget(props) {
  return (
    <ThemeWrapper>
      <RadioGroupWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledRadioGroupWidget as any).propTypes = {
  value: PropTypes.string,
  widgetAttributes: PropTypes.object,
  propertyName: PropTypes.string,
  onChange: PropTypes.func,
};
