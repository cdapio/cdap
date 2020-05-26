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

export const PLUGIN_TYPES = Object.keys(GLOBALS.pluginTypeToLabel).filter(
  (t) => t !== 'sqljoiner' && t !== 'batchjoiner' && t !== 'errortransform'
);

const widget_types = Object.keys(WIDGET_FACTORY);
widget_types.push('hidden');
export const WIDGET_TYPES = widget_types;

export const WIDGET_TYPE_TO_ATTRIBUTES = {
  'connection-browser': {
    connectionType: { type: 'string', required: true },
    label: { type: 'string', required: true },
  },
  csv: {
    // no attributes according to the docs
    'value-placeholder': { type: 'string', required: false },
    delimeter: { type: 'string', required: false },
  },
  'dataset-selector': {
    // no attributes according to the docs
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
    // only delimeter according to the docs
    'value-placeholder': { type: 'string', required: false },
    delimeter: { type: 'string', required: false },
  },
  'function-dropdown-with-alias': {
    /*placeholders?: Record<string, string>;
    dropdownOptions: IDropdownOption[];
    delimiter?: string;*/
  },
  dlp: {
    transforms: { type: 'ITransformProp[]', required: true },
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
    // only delimeter, dropdownOptions, kv-delimeter according to the docs
    'key-placeholder': { type: 'string', required: false },
    'kv-delimiter': { type: 'string', required: false },
    dropdownOptions: { type: 'IDropdownOption[]', required: true },
    delimiter: { type: 'string', required: false },
  },
  'keyvalue-encoded': {
    // NOT SURE
  },
  keyvalue: {
    // only delimeter and kv-delimeter according to the docs
    'key-placeholder': { type: 'string', required: false },
    'value-placeholder': { type: 'string', required: false },
    'kv-delimiter': { type: 'string', required: false },
    delimiter: { type: 'string', required: false },
    // ones that I found from json file
    showDelimiter: { type: 'boolean', required: false },
  },
  'memory-dropdown': {
    // NOT SURE
  },
  'memory-textbox': {
    // NOT SURE
  },
  'multi-select': {
    delimiter: { type: 'string', required: true },
    options: { type: 'IOption[]', required: true },
    showSelectionCount: { type: 'boolean', required: false },
    defaultValue: { type: 'string[]', required: false }, // from the doc
  },
  number: {
    min: { type: 'number', required: false },
    max: { type: 'number', required: false },
    default: { type: 'number', required: false }, // from the docs
  },
  password: {
    // no attributes according to the docs
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
    // NOT SURE
  },
  'securekey-text': {
    // NOT SURE
  },
  'securekey-textarea': {
    // NOT SURE
  },
  select: {
    options: { type: 'ISelectOptions[]|string[]|number[]', required: true },
    default: { type: 'string', required: false }, // from the docs
  },
  text: {
    placeholder: { type: 'string', required: false },
  },
  textbox: {
    placeholder: { type: 'string', required: false },
    default: { type: 'string', required: false }, // from the docs
  },
  toggle: {
    on: { type: 'IToggle', required: true },
    off: { type: 'IToggle', required: true },
    default: { type: 'string', required: false },
  },

  // CODE EDITORS
  'javascript-editor': {
    default: { type: 'string', required: false }, // from the doc
  },
  'json-editor': {
    default: { type: 'string', required: false }, // from the doc
  },
  'python-editor': {
    default: { type: 'string', required: false }, // from the doc
  },
  'scala-editor': {
    default: { type: 'string', required: false }, // from the doc
  },
  'sql-editor': {
    default: { type: 'string', required: false }, // from the doc
  },
  textarea: {
    placeholder: { type: 'string', required: false }, // from the doc
    rows: { type: 'number', required: false }, // from the docs
  },

  // JOINS
  'join-types': {},
  'sql-conditions': {},
  'sql-select-fields': {},

  // Rules Engine
  'rules-engine-editor': {},

  // Wrangler
  'wrangler-directives': {},

  // extra ones found from the docs
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

export const SCHEMA_TYPES = [
  'boolean',
  'bytes',
  'date',
  'double',
  'decimal',
  'float',
  'int',
  'long',
  'string',
  'time',
  'timestamp',
  'array',
  'enum',
  'map',
  'union',
  'record',
];

export const FILTER_CONDITION_PROPERTIES = ['property', 'operator', 'value', 'expression'];
export const OPERATOR_VALUES = Object.values(CustomOperator);
export const SHOW_TYPE_VALUES = Object.values(PropertyShowConfigTypeEnums);

export const COMMON_DELIMITER = ',';
