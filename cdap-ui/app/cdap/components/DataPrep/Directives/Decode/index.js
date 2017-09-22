/*
 * Copyright © 2017 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React from 'react';
import EncodeDecode from 'components/DataPrep/Directives/EncodeDecode';
import T from 'i18n-react';

const PREFIX = 'features.DataPrep.Directives.Decode';

const DECODEOPTIONS = [
  {
    label: T.translate(`${PREFIX}.base64`),
    getDirective: (column) => `decode base64 ${column}`
  },
  {
    label: T.translate(`${PREFIX}.base32`),
    getDirective: (column) => `decode base32 ${column}`
  },
  {
    label: T.translate(`${PREFIX}.hex`),
    getDirective: (column) => `decode hex ${column}`
  },
  {
    label: T.translate(`${PREFIX}.urldecode`),
    getDirective: (column) => `url-decode ${column}`
  }
];
export default function Decode({onComplete, column, isOpen}) {
  return (
    <EncodeDecode
      options={DECODEOPTIONS}
      directive="decode"
      onComplete={onComplete}
      column={column}
      isOpen={isOpen}
      mainMenuLabel={T.translate(`${PREFIX}.title`)}
    />
  );
}

Decode.propTypes = {
  onComplete: PropTypes.func,
  column: PropTypes.string,
  isOpen: PropTypes.bool
};
