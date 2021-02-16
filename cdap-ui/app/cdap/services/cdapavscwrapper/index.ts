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
import { LogicalTypes } from 'services/cdapavscwrapper/LogicalTypes';
import DateLogicalType from 'services/cdapavscwrapper/DateLogicalType';
import TimestampMicrosLogicalType from 'services/cdapavscwrapper/TimestampMicrosLogicalType';
import TimeMicrosLogicalType from 'services/cdapavscwrapper/TimeMicrosLogicalType';
import DecimalLogicalType from 'services/cdapavscwrapper/DecimalLogicalType';
import DateTimeLogicalType from 'services/cdapavscwrapper/DateTimeLogicalType';
import invert from 'lodash/invert';

// this dictionary is keeping the real AVRO logical type as the key
const LogicalTypesDictionary = {
  [LogicalTypes.DATE]: DateLogicalType,
  [LogicalTypes.TIMESTAMP_MICROS]: TimestampMicrosLogicalType,
  [LogicalTypes.TIME_MICROS]: TimeMicrosLogicalType,
  [LogicalTypes.DECIMAL]: DecimalLogicalType,
  [LogicalTypes.DATETIME]: DateTimeLogicalType,
};

// this is mapping the UI display type to the AVRO logical type
enum UI_TYPES {
  DATE = 'date',
  TIME = 'time',
  TIMESTAMP = 'timestamp',
  DECIMAL = 'decimal',
  DATETIME = 'datetime',
}

type IUiToAvro = { [key in UI_TYPES]: LogicalTypes };

type IAvroToUi = { [key in LogicalTypes]: UI_TYPES };

const UI_TO_AVRO_MAPPING: IUiToAvro = {
  [UI_TYPES.DATE]: LogicalTypes.DATE,
  [UI_TYPES.TIME]: LogicalTypes.TIME_MICROS,
  [UI_TYPES.TIMESTAMP]: LogicalTypes.TIMESTAMP_MICROS,
  [UI_TYPES.DECIMAL]: LogicalTypes.DECIMAL,
  [UI_TYPES.DATETIME]: LogicalTypes.DATETIME,
};

const AVRO_TO_UI_MAPPING: IAvroToUi = invert(UI_TO_AVRO_MAPPING) as IAvroToUi;

const CdapAvscWrapper = {
  parse: (schema, opts: object) => {
    const options = {
      ...opts,
      logicalTypes: LogicalTypesDictionary,
    };

    return cdapavsc.parse(schema, options);
  },

  formatType: (type) => {
    const logicalAvroType = UI_TO_AVRO_MAPPING[type];
    if (logicalAvroType) {
      return LogicalTypesDictionary[logicalAvroType].exportType();
    }

    return type;
  },

  getDisplayType: (type) => {
    if (AVRO_TO_UI_MAPPING[type]) {
      return AVRO_TO_UI_MAPPING[type];
    }

    return type;
  },
};

export default CdapAvscWrapper;
