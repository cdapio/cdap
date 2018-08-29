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

import AbstractLogicalType, { IJsonResponse } from 'services/cdapavscwrapper/AbstractLogicalType';
import { LogicalTypes } from 'services/cdapavscwrapper/LogicalTypes';
import cdapavsc from 'cdap-avsc';

const UNDERLYING_TYPE = 'int';

const JSON_FORMAT: IJsonResponse = {
  type: UNDERLYING_TYPE,
  logicalType: LogicalTypes.DATE,
};

export default class DateLogicalType extends AbstractLogicalType {
  constructor(attrs, opts) {
    super(attrs, opts, [cdapavsc.types.IntType], JSON_FORMAT);
  }

  public getTypeName(): string {
    return LogicalTypes.DATE;
  }

  public static exportType(): IJsonResponse {
    return JSON_FORMAT;
  }
}
