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
import AbstractWidget from './index';

export default {
  component: AbstractWidget,
  title: 'Widgets',
};

const selectOptions = [
  { value: 'choice A', label: 'choice A' },
  { value: 'choice B', label: 'choice B' },
];

const aliasOptions = [
  { value: 'Avg', label: 'Avg' },
  { value: 'Count', label: 'Count' },
  { value: 'CollectList', label: 'CollectList' },
];

const transforms = [
  { label: 'transform1', name: 'name1', options: [{ name: 'widget1' }, { name: 'widget2' }] },
  { label: 'transform2', name: 'name2', options: [{ name: 'widget3' }, { name: 'widget4' }] },
];
const filters = [{ id: 'filter A', label: 'filter A' }, { id: 'filter B', label: 'filter B' }];

const radioOptions = [{ label: 'choice A', id: 'choice A' }, { label: 'choice B', id: 'choice B' }];

const widgetProps = {
  datasetSelect: { placeholder: 'placeholder text' },
  dlp: { transforms, filters },
  functionDropdown: {
    dropdownOptions: aliasOptions,
  },
  radio: { options: radioOptions },
  select: { options: selectOptions },
  toggle: {
    on: { value: 'on', label: 'on' },
    off: { value: 'off', label: 'off' },
  },
};

// TO DO: Mock API for MyDataPrepApi
export const connectionBrowser = () => <AbstractWidget type="connection-browser" />;

export const csv = () => <AbstractWidget type="csv" />;

export const datasetSelector = () => (
  <AbstractWidget type="dataset-selector" widgetProps={widgetProps.datasetSelect} />
);

export const multipleValues = () => <AbstractWidget type="ds-multiplevalues" />;

export const dsv = () => <AbstractWidget type="dsv" />;

export const functionDropdownWithAlias = () => (
  <AbstractWidget type="function-dropdown-with-alias" widgetProps={widgetProps.functionDropdown} />
);

export const dlp = () => <AbstractWidget type="dlp" widgetProps={widgetProps.dlp} />;

export const getSchema = () => <AbstractWidget type="get-schema" />;

export const inputFieldDropdown = () => <AbstractWidget type="input-field-selector" />;

export const keyValueDropdown = () => <AbstractWidget type="keyvalue-dropdown" />;

export const keyValueEncoded = () => <AbstractWidget type="keyvalue-encoded" />;

export const keyValue = () => <AbstractWidget type="keyvalue" />;

export const memoryDropdown = () => (
  <AbstractWidget type="memory-dropdown" widgetProps={widgetProps.select} />
);

export const memoryTextbox = () => <AbstractWidget type="memory-textbox" />;

export const multiSelect = () => <AbstractWidget type="multi-select" />;

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

// export const text = () => <AbstractWidget type="text" />;

export const textbox = () => <AbstractWidget type="textbox" />;

export const toggle = () => <AbstractWidget type="toggle" widgetProps={widgetProps.toggle} />;
