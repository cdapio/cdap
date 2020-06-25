/*
 * Copyright © 2020 Cask Data, Inc.
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

import { WIDGET_FACTORY } from 'components/AbstractWidget/AbstractWidgetFactory';
import { CustomOperator, PropertyShowConfigTypeEnums } from 'components/ConfigurationGroup/types';
import { GLOBALS } from 'services/global-constants';

export const PluginTypes = Object.keys(GLOBALS.pluginTypeToLabel).filter(
  (t) => t !== 'sqljoiner' && t !== 'batchjoiner' && t !== 'errortransform'
);

export const WIDGET_TYPES = Object.keys(WIDGET_FACTORY);
export const WIDGET_CATEGORY = ['plugin'];

// including additional property that was found from the docs
// (DOCS: https://docs.cdap.io/cdap/6.1.2/en/developer-manual/pipelines/developing-plugins/presentation-plugins.html)
export const WIDGET_TYPE_TO_ATTRIBUTES = {
  'connection-browser': {
    connectionType: { type: 'string', required: true },
    label: { type: 'string', required: true },
  },
  csv: {
    'value-placeholder': { type: 'string', required: false },
    delimeter: { type: 'string', required: false },
  },
  'dataset-selector': {
    placeholder: { type: 'string', required: false },
  },
  daterange: {},
  datetime: {},
  'ds-multiplevalues': {
    placeholders: { type: 'string[]', required: false },
    'values-delimiter': { type: 'string', required: false },
    numValues: { type: 'string|number', required: true },
    delimiter: { type: 'string', required: false },
  },
  dsv: {
    'value-placeholder': { type: 'string', required: false },
    delimeter: { type: 'string', required: false },
  },
  'function-dropdown-with-alias': {
    /*
    TODO: After confirming
    placeholders?: Record<string, string>;
    dropdownOptions: IDropdownOption[];
    delimiter?: string;
    */
  },
  dlp: {
    // transforms: { type: 'ITransformProp[]', required: true },
    filters: { type: 'FilterOption[]', required: true },
    delimiter: { type: 'string', required: false },
  },
  'get-schema': {
    position: { type: 'Position', required: false },
  },
  'input-field-selector': {
    // no attributes according to the docs
    multiselect: { type: 'boolean', required: false },
    allowedTypes: { type: 'string[]', required: false },
  },
  'keyvalue-dropdown': {
    'key-placeholder': { type: 'string', required: false },
    'kv-delimiter': { type: 'string', required: false },
    dropdownOptions: { type: 'IDropdownOption[]', required: true },
    delimiter: { type: 'string', required: false },
  },
  'keyvalue-encoded': {
    // TODO: After confirming
  },
  keyvalue: {
    'key-placeholder': { type: 'string', required: false },
    'value-placeholder': { type: 'string', required: false },
    'kv-delimiter': { type: 'string', required: false },
    delimiter: { type: 'string', required: false },
    // including additional property that was found from one of the json files
    showDelimiter: { type: 'boolean', required: false },
  },
  'memory-dropdown': {
    // TODO: After confirming
  },
  'memory-textbox': {
    // TODO: After confirming
  },
  'multi-select': {
    delimiter: { type: 'string', required: true },
    options: { type: 'IOption[]', required: true },
    showSelectionCount: { type: 'boolean', required: false },
    // including additional property that was found from the docs
    defaultValue: { type: 'string[]', required: false },
  },
  number: {
    min: { type: 'number', required: false },
    max: { type: 'number', required: false },
    // including additional property that was found from the docs
    default: { type: 'number', required: false },
  },
  password: {
    placeholder: { type: 'string', required: false },
  },
  'plugin-list': {
    'plugin-type': { type: 'string', required: true },
  },
  'radio-group': {
    layout: { type: 'string', required: true },
    options: { type: 'IOption[]', required: true },
    default: { type: 'string', required: false },
  },
  'securekey-password': {
    // TODO: After confirming
  },
  'securekey-text': {
    // TODO: After confirming
  },
  'securekey-textarea': {
    // TODO: After confirming
  },
  select: {
    options: { type: 'ISelectOptions[]|string[]|number[]', required: true },
    default: { type: 'string', required: false },
  },
  text: {
    placeholder: { type: 'string', required: false },
  },
  textbox: {
    placeholder: { type: 'string', required: false },
    default: { type: 'string', required: false },
  },
  toggle: {
    on: { type: 'IToggle', required: true },
    off: { type: 'IToggle', required: true },
    default: { type: 'string', required: false },
  },

  // CODE EDITORS
  'javascript-editor': {
    default: { type: 'string', required: false },
  },
  'json-editor': {
    default: { type: 'string', required: false },
  },
  'python-editor': {
    default: { type: 'string', required: false },
  },
  'scala-editor': {
    default: { type: 'string', required: false },
  },
  'sql-editor': {
    default: { type: 'string', required: false },
  },
  textarea: {
    // including additional property that was found from the docs
    placeholder: { type: 'string', required: false },
    rows: { type: 'number', required: false },
  },

  // JOINS
  // TODO: After confirming
  'join-types': {},
  'sql-conditions': {},
  'sql-select-fields': {},

  // Rules Engine
  // TODO: After confirming
  'rules-engine-editor': {},

  // Wrangler
  // TODO: After confirming
  'wrangler-directives': {},

  // extra ones found from the docs
  // TODO: After confirming
  hidden: {
    default: { type: 'string', required: false },
  },
  'non-editable-schema-editor': {
    // schema: schema that will be used as the output schema for the plugin
  },
  schema: {
    'schema-default-type': { type: 'string', required: true },
    'schema-types': { type: 'string[]', required: true },
  },
  'stream-selector': {},
  'textarea-validate': {
    placeholder: { type: 'string', required: false },
    // 'validate-endpoint': plugin function endpoint to hit to validate the contents of the textarea
    'validate-button-text': { type: 'string', required: true },
    'validate-success-message': { type: 'string', required: true },
  },
};

// Delimiter that will be used throughout keyvalue inputs in Plugin JSON Creator
export const COMMON_DELIMITER = ',';
export const COMMON_KV_DELIMITER = ';';

export const CODE_EDITORS = [
  'javascript-editor',
  'json-editor',
  'python-editor',
  'scala-editor',
  'sql-editor',
];

export const SPEC_VERSION = '1.5';

// FILTER PAGE
export const OPERATOR_VALUES = Object.values(CustomOperator);
export const SHOW_TYPE_VALUES = Object.values(PropertyShowConfigTypeEnums);

// OUTPUT PAGE
export enum SchemaType {
  Explicit = 'schema',
  Implicit = 'non-editable-schema-editor',
}

export enum JSONStatusMessage {
  Normal = '',
  Success = 'SUCCESS',
  Fail = 'FAILURE',
}
