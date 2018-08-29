/*
 * Copyright © 2018 Cask Data, Inc.
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

import cdapavsc from 'cdap-avsc';

export interface IJsonResponse {
  type: string;
  logicalType: string;
}

export default class AbstractLogicalType extends cdapavsc.types.LogicalType {
  private JSON_FORMAT;

  constructor(attrs, opts, types, jsonFormat) {
    super(attrs, opts, types);

    this.JSON_FORMAT = jsonFormat;
  }

  public toJSON(): IJsonResponse {
    return this.JSON_FORMAT;
  }

  /*
   * There is a problem with avsc parser for wrapped union. It does not handle
   * the case where a logical type name contains dash (ie. time-micros). This
   * function is to strip off the dash.
   */
  public getName(): string {
    let name = this.getTypeName();
    name = name.replace(/-/g, '');

    return name;
  }

  public getTypeName(): string {
    throw new Error('Implement getTypeName method in child');
  }

  public static exportType(): IJsonResponse {
    throw new Error('Implement exportType method in child');
  }
}
