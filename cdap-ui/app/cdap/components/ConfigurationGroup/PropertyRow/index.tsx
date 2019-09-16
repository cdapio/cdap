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
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import { IPluginProperty, IWidgetProperty } from 'components/ConfigurationGroup/types';
import classnames from 'classnames';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { isMacro } from 'services/helpers';
import MacroIndicator from 'components/ConfigurationGroup/MacroIndicator';
import { isEmpty, isEqual, xorWith } from 'lodash';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      marginBottom: '5px',
      padding: '10px',
    },
    row: {
      marginBottom: '10px',
      display: 'grid',
      gridTemplateColumns: '1fr 40px',
      alignItems: 'center',
    },
    macroRow: {
      backgroundColor: theme.palette.grey[500],
    },
    label: {
      backgroundColor: theme.palette.grey[500],
    },
    errorText: {
      color: theme.palette.red[50],
    },
  };
};

interface IPropertyRowProps extends WithStyles<typeof styles> {
  widgetProperty: IWidgetProperty;
  pluginProperty: IPluginProperty;
  value: string;
  onChange: (values) => void;
  extraConfig: any;
  disabled: boolean;
  errors?: IErrorObj[];
}

const EditorTypeWidgets = [
  'javascript-editor',
  'python-editor',
  'rules-engine-editor',
  'scala-editor',
  'sql-editor',
  'textarea',
  'wrangler-directives',
];

interface IState {
  isMacroTextbox: boolean;
}

class PropertyRowView extends React.Component<IPropertyRowProps, IState> {
  public static defaultProps = {
    pluginProperty: {},
  };

  public state = {
    isMacroTextbox:
      isMacro(this.props.value) && objectQuery(this.props.pluginProperty, 'macroSupported'),
  };

  public shouldComponentUpdate(nextProps) {
    const rule =
      nextProps.value !== this.props.value ||
      nextProps.widgetProperty['widget-type'] !== this.props.widgetProperty['widget-type'];
    // Comparison of array of objects
    const isArrayEqual = (x: IErrorObj[], y: IErrorObj[]) => isEmpty(xorWith(x, y, isEqual));
    const errorChange = isArrayEqual(nextProps.errors, this.props.errors);

    return rule || !errorChange;
  }

  private toggleMacro = () => {
    if (this.props.disabled) {
      return;
    }
    const newValue = !this.state.isMacroTextbox;

    if (newValue) {
      this.handleChange('${}');
    } else {
      this.handleChange('');
    }

    this.setState({ isMacroTextbox: newValue });
  };

  private handleChange = (value) => {
    const newValues = {
      ...this.props.extraConfig.properties,
      [this.props.widgetProperty.name]: value,
    };

    this.props.onChange(newValues);
  };

  private updateAllProperties = (values) => {
    const newValues = {
      ...this.props.extraConfig.properties,
      ...values,
    };

    this.props.onChange(newValues);
  };

  public render() {
    const {
      classes,
      pluginProperty,
      value,
      disabled,
      extraConfig,
      widgetProperty,
      errors,
    } = this.props;

    if (widgetProperty['widget-type'] === 'hidden') {
      return null;
    }

    let widgetClasses;
    const updatedWidgetProperty = {
      ...widgetProperty,
    };
    if (this.state.isMacroTextbox) {
      const currentWidget = updatedWidgetProperty['widget-type'];
      if (EditorTypeWidgets.indexOf(currentWidget) === -1) {
        updatedWidgetProperty['widget-type'] = 'textbox';
        updatedWidgetProperty['widget-attributes'] = {};
      }

      widgetClasses = {
        label: classes.label,
      };
    }

    // When there is only one error and it does not have element property,
    // it is a property level error.
    const propertyLevelErrorMsg =
      errors && errors.length === 1 && !errors[0].element ? errors[0].msg : '';
    // Fix styling of error here.
    return (
      <div className={classes.root}>
        <div className={classnames(classes.row, { [classes.macroRow]: this.state.isMacroTextbox })}>
          <WidgetWrapper
            widgetProperty={updatedWidgetProperty}
            pluginProperty={pluginProperty}
            value={value || ''}
            onChange={this.handleChange}
            updateAllProperties={this.updateAllProperties}
            extraConfig={extraConfig}
            classes={widgetClasses}
            disabled={disabled}
            errors={errors}
          />
          <If condition={pluginProperty.macroSupported}>
            <MacroIndicator
              onClick={this.toggleMacro}
              disabled={disabled}
              isActive={this.state.isMacroTextbox}
            />
          </If>
        </div>
        <If condition={propertyLevelErrorMsg !== ''}>
          <div className={classnames(classes.errorText, classes.errorRow)}>
            {propertyLevelErrorMsg}
          </div>
        </If>
      </div>
    );
  }
}

const PropertyRow = withStyles(styles)(PropertyRowView);
export default PropertyRow;
