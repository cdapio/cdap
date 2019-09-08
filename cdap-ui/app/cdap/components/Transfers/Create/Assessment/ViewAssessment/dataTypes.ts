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

export enum BQ_TYPES {
  STRING = 'STRING',
  BYTES = 'BYTES',
  INTEGER = 'INTEGER',
  FLOAT = 'FLOAT',
  NUMERIC = 'NUMERIC',
  BOOLEAN = 'BOOLEAN',
  TIMESTAMP = 'TIMESTAMP',
  DATE = 'DATE',
  TIME = 'TIME',
  DATETIME = 'DATETIME',
  GEOGRAPHY = 'GEOGRAPHY',
  RECORD = 'RECORD',
  ARRAY = 'ARRAY',
  STRUCT = 'STRUCT',
}

export enum MYSQL_TYPES {
  INTEGER = 'INTEGER',
  INT = 'INT',
  SMALLINT = 'SMALLINT',
  TINYINT = 'TINYINT',
  MEDIUMINT = 'MEDIUMINT',
  BIGINT = 'BIGINT',
  DECIMAL = 'DECIMAL',
  NUMERIC = 'NUMERIC',
  FLOAT = 'FLOAT',
  DOUBLE = 'DOUBLE',
  BIT = 'BIT',
  DATE = 'DATE',
  DATETIME = 'DATETIME',
  TIMESTAMP = 'TIMESTAMP',
  TIME = 'TIME',
  YEAR = 'YEAR',
  CHAR = 'CHAR',
  VARCHAR = 'VARCHAR',
  BINARY = 'BINARY',
  VARBINARY = 'BINARY',
  BLOB = 'BLOB',
  TEXT = 'TEXT',
  ENUM = 'ENUM',
  SET = 'SET',
  JSON = 'JSON',
}
