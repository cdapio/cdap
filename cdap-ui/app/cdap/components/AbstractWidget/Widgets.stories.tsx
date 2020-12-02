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

import * as React from 'react';
import { action } from '@storybook/addon-actions';
import { withInfo } from '@storybook/addon-info';
import AbstractWidget from './index';

export default {
  component: AbstractWidget,
  title: 'Widgets',
  decorators: [withInfo],
};

const memoryOptions = [1024, 2048];

const selectOptions = [
  { value: 'choice A', label: 'choice A' },
  { value: 'choice B', label: 'choice B' },
];

const aliasOptions = [
  { value: 'Avg', label: 'Avg' },
  { value: 'Count', label: 'Count' },
  { value: 'CollectList', label: 'CollectList' },
];

const idLabelOptions = [
  { id: 'avg', label: 'Avg' },
  { id: 'count', label: 'Count' },
  { id: 'collectionList', label: 'CollectionList' },
];

const radioOptions = [
  { label: 'choice A', id: 'choice A' },
  { label: 'choice B', id: 'choice B' },
  { label: 'choice C', id: 'choice C' },
];

const widgetProps = {
  functionDropdown: {
    dropdownOptions: aliasOptions,
  },
  functionDropdownIdLabel: {
    dropdownOptions: idLabelOptions,
  },
  keyValueDropdown: {
    dropdownOptions: ['simple option 1', 'simple option 2', 'simple option 3'],
  },
  memorySelect: {
    values: memoryOptions,
  },
  radio: { options: radioOptions },
  select: { options: selectOptions },
  textbox: { placeholder: 'placeholder text' },
  toggle: {
    on: { value: 'on', label: 'on' },
    off: { value: 'off', label: 'off' },
  },
};

export const csv = () => <AbstractWidget type="csv" />;

export const daterange = () => <AbstractWidget type="daterange" />;

export const datetime = () => <AbstractWidget type="datetime" />;

export const multipleValues = () => <AbstractWidget type="ds-multiplevalues" />;

export const dsv = () => <AbstractWidget type="dsv" />;

export const functionDropdownWithAlias = () => (
  <AbstractWidget type="function-dropdown-with-alias" widgetProps={widgetProps.functionDropdown} />
);

export const functionDropdownWithArguments = () => (
  <AbstractWidget
    type="function-dropdown-with-arguments"
    widgetProps={widgetProps.functionDropdownIdLabel}
  />
);

export const keyValueDropdown = () => (
  <AbstractWidget type="keyvalue-dropdown" widgetProps={widgetProps.keyValueDropdown} />
);

export const keyValueEncoded = () => <AbstractWidget type="keyvalue-encoded" />;

export const keyValue = () => <AbstractWidget type="keyvalue" />;

export const memoryDropdown = () => (
  <AbstractWidget type="memory-dropdown" widgetProps={widgetProps.memorySelect} />
);

export const memoryTextbox = () => <AbstractWidget type="memory-textbox" />;

export const multiSelect = () => (
  <AbstractWidget type="multi-select" widgetProps={widgetProps.radio} />
);

export const numberTextbox = () => <AbstractWidget type="number" />;

export const password = () => <AbstractWidget type="password" />;

export const radioGroup = () => (
  <AbstractWidget type="radio-group" widgetProps={widgetProps.radio} value="choice A" />
);

export const secureKeyPassword = () => <AbstractWidget type="securekey-password" />;

export const secureKeyText = () => <AbstractWidget type="securekey-text" />;

export const secureKeyTextArea = () => <AbstractWidget type="securekey-textarea" />;

export const selectDropdown = () => (
  <AbstractWidget
    value={'choice A'}
    type="select"
    widgetProps={widgetProps.select}
    onChange={action('onChange')}
  />
);

export const textbox = () => <AbstractWidget type="textbox" widgetProps={widgetProps.textbox} />;

export const toggle = () => <AbstractWidget type="toggle" widgetProps={widgetProps.toggle} />;
