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

import { CustomOperator, PropertyShowConfigTypeEnums } from 'components/ConfigurationGroup/types';

import { GLOBALS } from 'services/global-constants';
import { WIDGET_FACTORY } from 'components/AbstractWidget/AbstractWidgetFactory';

export const PluginTypes = Object.keys(GLOBALS.pluginTypeToLabel).filter(
  (t) => t !== 'sqljoiner' && t !== 'batchjoiner' && t !== 'errortransform'
);

export const WIDGET_TYPES = Object.keys(WIDGET_FACTORY).filter((t) => t !== 'dlp');

export const WIDGET_CATEGORY = ['plugin'];

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
