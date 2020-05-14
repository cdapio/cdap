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

import AbstractLogicalType, { IJsonResponse } from 'services/cdapavscwrapper/AbstractLogicalType';
import { LogicalTypes } from 'services/cdapavscwrapper/LogicalTypes';
import cdapavsc from 'cdap-avsc';
import isNil from 'lodash/isNil';

const UNDERLYING_TYPE = 'bytes';
interface IDecimalJsonResponse extends IJsonResponse {
  precision?: number;
  scale?: number;
}
const JSON_FORMAT: IDecimalJsonResponse = {
  type: UNDERLYING_TYPE,
  logicalType: LogicalTypes.DECIMAL,
};

export default class DecimalLogicalType extends AbstractLogicalType {
  constructor(attrs, opts) {
    if (!isNil(attrs.precision)) {
      JSON_FORMAT.precision = attrs.precision;
    }
    if (!isNil(attrs.scale)) {
      JSON_FORMAT.scale = attrs.scale;
    }
    super(attrs, opts, [cdapavsc.types.LongType], JSON_FORMAT);
  }

  public getTypeName(): string {
    return LogicalTypes.DECIMAL;
  }

  public static exportType(): IJsonResponse {
    return JSON_FORMAT;
  }
}
