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
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';
import MultiSelect, { IOption } from 'components/AbstractWidget/FormInputs/MultiSelect';
import {
  IWidgetProperty,
  IPropertyFilter,
  IWidgetJson,
  PluginProperties,
} from 'components/ConfigurationGroup/types';
import InputFieldDropdown from 'components/AbstractWidget/InputFieldDropdown';
import { IconButton } from '@material-ui/core';
import If from 'components/If';
import ExpandLessIcon from '@material-ui/icons/ExpandLess';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';
import {
  extractAndSplitMatchingErrors,
  parseNestedErrors,
  parseTransformOptions,
} from './utilities/index';
import T from 'i18n-react';

const styles = (theme): StyleRules => {
  return {
    ...AbstractRowStyles(theme),
    root: {
      minHeight: '50px',
      display: 'grid',
      gridTemplateColumns: '1fr auto auto auto',
      alignItems: 'center',
      '& > button': {
        width: '40px',
        height: '40px',
        margin: 'auto',
        marginTop: '5px',
      },
    },
    inputContainer: {
      display: 'flex',
      flexDirection: 'row',
      flexWrap: 'wrap',
      alignItems: 'baseline',
      minHeight: '27px',
    },
    errorBorder: {
      border: '2px solid',
      borderColor: theme.palette.red[50],
    },
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
    separator: {
      textAlign: 'center',
      display: 'inline-flex',
      alignItems: 'center',
    },
    group: {
      marginLeft: '20px',
      marginTop: '15px',
      marginBottom: '15px',
      '& > div > div': {
        padding: '15px 10px 10px',
      },
    },
    groupTitle: {
      marginBottom: '15px',
      '& > h2': {
        fontSize: '1.2rem',
      },
    },
    errorText: {
      color: theme.palette.red[50],
      marginLeft: '15px',
      marginTop: '5px',
    },
    labelSelectWrapper: {
      display: 'flex',
      flexGrow: 1,
      minWidth: '125px',
      marginRight: '5px',
      padding: '3px 0px',
      '& > div': {
        minHeight: '27px',
        width: '90%',
        flexGrow: 1,
        marginLeft: '10px',
        marginRight: '10px',
      },
    },
  };
};

export interface IErrorConfig {
  transform: string;
  fields: string;
  filters: string;
  transformPropertyId: string;
  isNestedError: boolean;
}

export interface ITransformProp {
  label: string;
  name: string;
  options: IWidgetProperty[];
  supportedTypes?: string[];
  filters?: IPropertyFilter[];
}
export type FilterOption = IOption | string;

export interface IDLPRowProps extends IAbstractRowProps<typeof styles> {
  transforms: ITransformProp[];
  filters: FilterOption[];
  extraConfig: any;
}

export interface IDLPRowState {
  fields: string;
  transform: string;
  filters: string;
  transformProperties: Record<string, string>;
  expanded?: boolean;
  transformObj?: ITransformProp;
}

type StateKeys = keyof IDLPRowState;
class DLPRow extends AbstractRow<IDLPRowProps, IDLPRowState> {
  public static defaultProps = {
    transforms: [],
    filters: [],
    extraConfig: '',
    expanded: false,
  };

  public state = {
    fields: '',
    transform: '',
    filters: '',
    transformProperties: {},
    expanded: false,
    transformObj: null,
  };

  public configurationGroupPluginProperties: PluginProperties;
  public configurationGroupWidgetJson: IWidgetJson;

  public componentDidMount() {
    try {
      const jsonString = this.props.value;
      const jsonObj = JSON.parse(jsonString);
      jsonObj.transformObj =
        this.props.transforms.filter((t) => t.name === jsonObj.transform)[0] || null;
      this.configurationGroupPluginProperties = parseTransformOptions(jsonObj.transformObj);
      this.configurationGroupWidgetJson = {};
      if (jsonObj.transformObj !== null) {
        this.configurationGroupWidgetJson = {
          'configuration-groups': [
            {
              label: jsonObj.transformObj.label + ' properties',
              properties: jsonObj.transformObj.options,
            },
          ],
          filters: jsonObj.transformObj.filters, // Passing ConfigurationGroup filters to allow for dynamic showing/hiding of properties
        };
      }
      this.setState({ ...this.state, ...jsonObj });
    } catch (error) {
      this.setState({
        fields: '',
        transform: '',
        filters: '',
        transformProperties: {},
      });
    }
  }

  // Handles onChange for MultiSelect components (filters and fields selectors)
  private handleChangeMultiSelect = (type: StateKeys, e: string) => {
    if (type === 'filters') {
      const noneWasSelected: boolean = this.state.filters.includes('NONE');
      const noneIsSelected: boolean = e.includes('NONE');

      // Ensuring user can't select 'NONE' along with other filters
      // TODO: Use another component for the filters selector (https://issues.cask.co/browse/CDAP-16124)
      if (noneWasSelected && noneIsSelected) {
        e = e.replace('NONE,', '');
      } else if (!noneWasSelected && noneIsSelected) {
        e = 'NONE';
      }
    }

    this.handleChange({ [type]: e });
  };

  // Handles onChange for Select components (transform selector)
  private handleChangeTransformSelect = (type: StateKeys, e) => {
    const value = e.target.value;
    if (value === this.state.transform) {
      return;
    }
    const newValues = { [type]: value };

    const transformProps = {};
    newValues.transformObj = this.props.transforms.filter((t) => t.name === value)[0] || null;
    if (newValues.transformObj !== null) {
      newValues.transformObj.options.forEach((element) => {
        transformProps[element.name] = element['widget-attributes'].default || '';
      });
    }

    newValues.transformProperties = transformProps;
    newValues.expanded = Object.keys(transformProps).length > 0;
    this.configurationGroupPluginProperties = parseTransformOptions(newValues.transformObj);

    this.configurationGroupWidgetJson = {};
    if (newValues.transformObj !== null) {
      this.configurationGroupWidgetJson = {
        'configuration-groups': [
          {
            label: newValues.transformObj.label + ' properties',
            properties: newValues.transformObj.options,
          },
        ],
        filters: newValues.transformObj.filters, // Passing ConfigurationGroup filters to allow for dynamic showing/hiding of properties
      };
    }

    this.handleChange(newValues);
  };

  private handleChangeTransformOptions = (values: Record<string, string>) => {
    // Set values in state if they were cleared by the configuration group
    if (this.state.transformObj) {
      this.state.transformObj.options.forEach((option: IWidgetProperty) => {
        if (values[option.name] === undefined) {
          values[option.name] = option['widget-attributes'].default || '';
        }
      });
    }

    this.handleChange({ transformProperties: values });
  };

  private handleChange = (newValues) => {
    this.setState(newValues, () => {
      const { transform } = this.state;

      if (transform.length === 0) {
        this.onChange('');
        return;
      }

      const updatedValue = {
        fields: this.state.fields,
        transform: this.state.transform,
        filters: this.state.filters,
        transformProperties: this.state.transformProperties,
      };
      this.onChange(JSON.stringify(updatedValue));
    });
  };

  public renderInput = () => {
    // Parsing filters
    const filters = this.props.filters.map((option: FilterOption) => {
      if (typeof option === 'object') {
        return option;
      }

      return {
        id: option,
        label: option,
      };
    });

    // Splitting erros between localErrors (errors that apply to the transform, filters or fields)
    // and nestedErrors (errors that apply to the nested transform properties)
    let localErrors: IErrorObj[] = [];
    let nestedErrors: IErrorObj[] = [];
    [nestedErrors, localErrors] = extractAndSplitMatchingErrors(this.state, this.props.errors);

    const transforms = this.props.transforms;

    // WidgetProps for inputFieldDropdown
    const inputFieldProps = {
      multiselect: true,
      allowedTypes: [],
    };

    if (this.state.transform !== '') {
      if (nestedErrors.length > 0) {
        this.handleChange({ expanded: true });
      }

      inputFieldProps.allowedTypes = this.state.transformObj.supportedTypes || [];
    }

    const shouldShowConfigGroup =
      this.state.transform !== '' &&
      Object.keys(this.state.transformProperties).length > 0 &&
      this.state.expanded &&
      this.state.transformObj !== null;

    const classes = this.props.classes;

    // TODO: Align the select componments when in multi-row config
    return (
      <React.Fragment>
        <div>
          <div className={classes.inputContainer}>
            <div className={classes.labelSelectWrapper}>
              <span className={classes.separator}>
                {T.translate('features.AbstractWidget.DLPWidget.apply')}
              </span>
              <Select
                classes={{ disabled: classes.disabled }}
                value={this.state.transform}
                onChange={this.handleChangeTransformSelect.bind(this, 'transform')}
                displayEmpty={true}
                disabled={false}
              >
                {transforms.map((option) => {
                  return (
                    <MenuItem value={option.name} key={option.name}>
                      {option.label}
                    </MenuItem>
                  );
                })}
              </Select>
            </div>
            <div className={classes.labelSelectWrapper}>
              <span className={classes.separator}>
                {T.translate('features.AbstractWidget.DLPWidget.on')}
              </span>
              <MultiSelect
                disabled={false}
                value={this.state.filters}
                widgetProps={{ options: filters, showSelectionCount: true }}
                onChange={this.handleChangeMultiSelect.bind(this, 'filters')}
              />
            </div>

            <div className={classes.labelSelectWrapper}>
              <span className={classes.separator}>
                {T.translate('features.AbstractWidget.DLPWidget.within')}
              </span>
              <InputFieldDropdown
                widgetProps={inputFieldProps}
                value={this.state.fields}
                onChange={this.handleChangeMultiSelect.bind(this, 'fields')}
                disabled={false}
                extraConfig={this.props.extraConfig}
              />
            </div>
          </div>

          {localErrors.map((err) => {
            return (
              <div key={err.element} className={classes.errorText}>
                {err.msg}
              </div>
            );
          })}
          <div className={classes.transformContainer}>
            <If condition={shouldShowConfigGroup}>
              <ConfigurationGroup
                pluginProperties={this.configurationGroupPluginProperties}
                errors={parseNestedErrors(nestedErrors)}
                values={this.state.transformProperties}
                classes={classes}
                onChange={this.handleChangeTransformOptions}
                inputSchema={this.props.extraConfig.inputSchema}
                widgetJson={this.configurationGroupWidgetJson}
              />
            </If>
          </div>
        </div>

        <If condition={this.state.expanded}>
          <IconButton
            color="primary"
            onClick={() => {
              this.handleChange({ expanded: !this.state.expanded });
            }}
          >
            <ExpandLessIcon />
          </IconButton>
        </If>

        <If condition={!this.state.expanded}>
          <IconButton
            color="primary"
            onClick={() => {
              this.handleChange({ expanded: !this.state.expanded });
            }}
            disabled={Object.keys(this.state.transformProperties).length === 0}
          >
            <ExpandMoreIcon />
          </IconButton>
        </If>
      </React.Fragment>
    );
  };
}

const StyledDLPRow = withStyles(styles)(DLPRow);
export default StyledDLPRow;
