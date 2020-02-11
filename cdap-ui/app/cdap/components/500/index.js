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

import PropTypes from 'prop-types';
import React from 'react';
import { isNilOrEmpty } from 'services/helpers';
import { DEFAULT_ERROR_MESSAGE } from 'components/ErrorBoundary';
import Page500ErrorStack from 'components/500/Page500ErrorStack';
import T from 'i18n-react';
import If from 'components/If';

require('./500.scss');
const I18N_PREFIX = 'features.Page500';

export default function Page500({ message, stack, refreshFn }) {
  return (
    <div className="page-500">
      <h1 className="error-main-title">{T.translate(`${I18N_PREFIX}.mainTitle`)}</h1>
      <h1>
        <strong data-cy="page-500-error-msg">
          {typeof message === 'string' ? message : T.translate(`${I18N_PREFIX}.secondaryTitle`)}
        </strong>
      </h1>

      <div className="message-section">
        <h4>
          <strong>{T.translate('features.Page404.subtitleMessage1')}</strong>
        </h4>
        <div className="navigation-section">
          <div>
            {
              // There is definitely a better way to do this :sigh:
            }
            {T.translate(`${I18N_PREFIX}.suggestion1Part1`)}
            <If condition={typeof refreshFn === 'function'}>
              <span className="refreshBtn" onClick={refreshFn}>
                {T.translate(`${I18N_PREFIX}.suggestion1Part2`)}
              </span>
            </If>
            <If condition={typeof refreshFn !== 'function'}>
              <a href={window.location.href}>{T.translate(`${I18N_PREFIX}.suggestion1Part2`)}</a>
            </If>
            {T.translate(`${I18N_PREFIX}.suggestion1Part3`)}
          </div>
          <div>{T.translate(`${I18N_PREFIX}.suggestion2`)}</div>
        </div>
      </div>
      {isNilOrEmpty(stack) ? null : (
        <Page500ErrorStack
          stack={stack}
          message={isNilOrEmpty(message) ? DEFAULT_ERROR_MESSAGE : message}
        />
      )}
    </div>
  );
}
Page500.propTypes = {
  message: PropTypes.string,
  stack: PropTypes.object,
  refreshFn: PropTypes.func,
};
