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

/**
 * Please maintain alphabetical order of the widget factory.
 *
 * I have put the special widgets at the bottom (ie. Joiner and Wrangler)
 * and also grouped together all the code editor types.
 */
export const WIDGET_FACTORY = {
  'connection-browser': 'PluginConnectionBrowser',
  csv: 'CSVWidget',
  'dataset-selector': 'DatasetSelector',
  daterange: 'DateRangeWidget',
  datetime: 'DateTimeWidget',
  'ds-multiplevalues': 'MultipleValuesWidget',
  dsv: 'CSVWidget',
  'function-dropdown-with-alias': 'FunctionDropdownAliasWidget',
  dlp: 'DLPCustomWidget',
  'get-schema': 'GetSchemaWidget',
  'input-field-selector': 'InputFieldDropdown',
  'keyvalue-dropdown': 'KeyValueDropdownWidget',
  'keyvalue-encoded': 'KeyValueEncodedWidget',
  keyvalue: 'KeyValueWidget',
  'memory-dropdown': 'MemorySelectWidget',
  'memory-textbox': 'MemoryTextbox',
  'multi-select': 'MultiSelect',
  number: 'NumberWidget',
  password: 'PasswordWidget',
  'plugin-list': 'PluginListWidget',
  'radio-group': 'RadioGroupWidget',
  'securekey-password': 'SecureKeyPassword',
  'securekey-text': 'SecureKeyText',
  'securekey-textarea': 'SecureKeyTextarea',
  select: 'Select',
  text: 'TextBox',
  textbox: 'TextBox',
  toggle: 'ToggleSwitchWidget',

  // CODE EDITORS
  'javascript-editor': `(props) => {
    return <CodeEditorWidget mode="javascript" rows={25} {...props} />;
  }`,
  'json-editor': `(props) => {
    const rows = getRowsFromWidgetProps(props);
    return <JsonEditorWidget rows={rows} {...props} />;
  }`,
  'python-editor': `(props) => {
    return <CodeEditorWidget mode="python" rows={25} {...props} />;
  }`,
  'scala-editor': `(props) => {
    return <CodeEditorWidget mode="scala" rows={25} {...props} />;
  }`,
  'sql-editor': `(props) => {
    return <CodeEditorWidget mode="sql" rows={15} {...props} />;
  }`,
  textarea: `(props) => {
    const rows = getRowsFromWidgetProps(props);
    return <CodeEditorWidget mode="plain_text" rows={rows} {...props} />;
  }`,

  // JOINS
  'join-types': 'JoinTypeWidget',
  'sql-conditions': 'SqlConditionsWidget',
  'sql-select-fields': 'SqlSelectorWidget',

  // Rules Engine
  'rules-engine-editor': 'RulesEngineEditor',

  // Wrangler
  'wrangler-directives': 'WranglerEditor',
};
