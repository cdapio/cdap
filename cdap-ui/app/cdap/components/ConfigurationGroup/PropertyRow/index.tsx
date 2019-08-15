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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import AbstractWidget from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import { IPluginProperty, IWidgetProperty } from '../types';

const styles = (): StyleRules => {
  return {
    row: {
      marginBottom: '15px',
    },
  };
};

interface IPropertyRowProps extends WithStyles<typeof styles> {
  widgetProperty: IWidgetProperty;
  pluginProperty: IPluginProperty;
  value: string;
  onChange: (value: string) => void;
  extraConfig: any;
}

const PropertyRowView: React.FC<IPropertyRowProps> = ({
  widgetProperty,
  pluginProperty,
  value,
  onChange,
  extraConfig,
  classes,
}) => {
  const widgetType = objectQuery(widgetProperty, 'widget-type');
  if (widgetType === 'hidden') {
    return null;
  }

  return (
    <div className={classes.row}>
      <label className="control-label">{widgetProperty.label}</label>
      <AbstractWidget
        type={widgetType}
        value={value || ''}
        onChange={onChange}
        widgetProps={widgetProperty['widget-attributes']}
        extraConfig={extraConfig}
      />
      {/* Temporary macro indicator */}
      <If condition={pluginProperty.macroSupported}>
        <div>
          <span className="badge badge-secondary">Macro</span>
        </div>
      </If>
    </div>
  );
};

const PropertyRow = withStyles(styles)(PropertyRowView);
export default PropertyRow;
