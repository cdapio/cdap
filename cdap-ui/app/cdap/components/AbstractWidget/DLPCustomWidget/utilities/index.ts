import { ITransformProp, IErrorConfig, IDLPRowState, IDLPRowProps } from '../DLPRow';
import { PluginProperties, IPluginProperty } from 'components/ConfigurationGroup/types';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';
import { IConfigurationGroupProps } from 'components/ConfigurationGroup';

export function parseTransformOptions(transform: ITransformProp) {
  if (transform == null) {
    return {};
  }
  const properties: PluginProperties = {};
  // Populating the pluginProperties for each transform property
  transform.options.forEach((transProp) => {
    const pluginProp: IPluginProperty = {
      name: transProp.name,
    };
    if (transProp['widget-attributes']) {
      if (transProp['widget-attributes'].macro) {
        pluginProp.macroSupported = transProp['widget-attributes'].macro;
      }
      if (transProp['widget-attributes'].description) {
        pluginProp.description = transProp['widget-attributes'].description;
      }
      if (transProp['widget-attributes'].required) {
        pluginProp.required = transProp['widget-attributes'].required;
      }
    }
    properties[transProp.name] = pluginProp;
  });
  return properties;
}

export function parseNestedErrors(nestedErrors: IErrorObj[]) {
  const errors = {};
  nestedErrors.forEach((err) => {
    try {
      const errorConfig: IErrorConfig = JSON.parse(err.element);
      const errorObj = { msg: err.msg };
      if (errorConfig.transformPropertyId in errors) {
        errors[errorConfig.transformPropertyId].push(errorObj);
      } else {
        errors[errorConfig.transformPropertyId] = [errorObj];
      }
    } catch (error) {
      return;
    }
  });

  return errors;
}

export function extractAndSplitMatchingErrors(state: IDLPRowState, errors: IErrorObj[]) {
  if (errors) {
    const localErrors: IErrorObj[] = [];
    const nestedErrors: IErrorObj[] = [];
    errors.forEach((err) => {
      try {
        const errorConfig: IErrorConfig = JSON.parse(err.element);
        if (
          errorConfig.fields === state.fields &&
          errorConfig.transform === state.transform &&
          errorConfig.filters === state.filters
        ) {
          if (errorConfig.isNestedError) {
            nestedErrors.push(err);
          } else {
            localErrors.push(err);
          }
        }
      } catch (error) {
        return null;
      }
    });
    return [nestedErrors, localErrors];
  }
  return [[], []];
}

export function getConfigurationGroupConfig(
  state: IDLPRowState,
  props: IDLPRowProps,
  transform: ITransformProp,
  nestedErrors: IErrorObj[],
  handleChange
) {
  const config: IConfigurationGroupProps = {
    classes: {},
    errors: {},
    values: {},
    pluginProperties: {},
  };

  if (state.transform !== '') {
    config.pluginProperties = parseTransformOptions(transform);
    config.errors = parseNestedErrors(nestedErrors);
    config.values = state.transformProperties;
    config.classes = props.classes;
    config.onChange = handleChange;
    config.inputSchema = props.extraConfig.inputSchema;
    config.widgetJson = {
      'configuration-groups': [
        {
          label: transform.label + ' properties',
          properties: transform.options,
        },
      ],
      filters: transform.filters, // Passing ConfigurationGroup filters to allow for dynamic showing/hiding of properties
    };
  }

  return config;
}
