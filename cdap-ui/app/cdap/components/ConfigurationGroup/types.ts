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

export interface IWidgetProperty {
  name: string;
  label?: string;
  'widget-type'?: string;
  'widget-attributes'?: any;
}

export interface IConfigurationGroup {
  label: string;
  description?: string;
  properties: IWidgetProperty[];
}
export enum CustomOperator {
  EXISTS = 'exists',
  DOESNOTEXISTS = 'does not exist',
  EQUALTO = 'equal to',
  NOTEQUALTO = 'not equal to',
}

type IPropertyFilterOperator =
  | CustomOperator.EQUALTO
  | CustomOperator.NOTEQUALTO
  | CustomOperator.EXISTS
  | CustomOperator.DOESNOTEXISTS;

export enum PropertyShowConfigTypeEnums {
  GROUP = 'group',
  PROPERTY = 'property',
}
export type IPropertyShowConfigTypes =
  | PropertyShowConfigTypeEnums.GROUP
  | PropertyShowConfigTypeEnums.PROPERTY;
interface IPropertyShowConfig {
  name: string;
  type?: IPropertyShowConfigTypes;
}

export type IPropertyValues = Record<string, string>;
export type IPropertyTypedValues = Record<string, IPropertyValueType>;
export type IPropertyValueType = boolean | number | string;

/**
 * Filter condition to show or hide a property.
 * Could be an javascript expression or a simple operator exposed by CDAP UI
 */
export interface IPropertyFilterCondition {
  property?: string;
  expression?: string;
  operator?: IPropertyFilterOperator;
  value?: string;
}

/**
 * Filter with @{name} and a @{condition} that decides to hide or @{show} a plugin property.
 */
export interface IPropertyFilter {
  name: string;
  condition: IPropertyFilterCondition;
  show: IPropertyShowConfig[];
}

export interface IWidgetJson {
  'configuration-groups'?: IConfigurationGroup[];
  filters?: IPropertyFilter[];
}

export interface IPluginProperty {
  name?: string;
  type?: string;
  required?: boolean;
  macroSupported?: boolean;
  description?: string;
}

export type PluginProperties = Record<string, IPluginProperty>;
