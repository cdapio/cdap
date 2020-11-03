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
 * This file is to add custom query for cypress data-cy attribute.
 */

import { queryHelpers, buildQueries } from '@testing-library/react';

// The queryAllByAttribute is a shortcut for attribute-based matchers
// You can also use document.querySelector or a combination of existing
// testing library utilities to find matching nodes for your query
const queryAllByCy = (...args) => {
  return queryHelpers.queryAllByAttribute('data-cy', ...args);
};

const getMultipleError = (c, dataCyValue) => {
  return `Found multiple elements with the data-cy attribute of: ${dataCyValue}`;
};

const getMissingError = (c, dataCyValue) => {
  return `Unable to find an element with the data-cy attribute of: ${dataCyValue}`;
};

const [queryByCy, getAllByCy, getByCy, findAllByCy, findByCy] = buildQueries(
  queryAllByCy,
  getMultipleError,
  getMissingError
);

export { queryByCy, queryAllByCy, getByCy, getAllByCy, findAllByCy, findByCy };
