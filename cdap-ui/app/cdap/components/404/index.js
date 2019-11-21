/*
 * Copyright © 2016 Cask Data, Inc.
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
import { Link } from 'react-router-dom';
import NamespaceStore from 'services/NamespaceStore';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
require('./404.scss');

const I18N_PREFIX = 'features.Page404';
export default function Page404({ entityName, entityType, children }) {
  let namespace = NamespaceStore.getState().selectedNamespace;
  return (
    <div className="page-not-found">
      <h1 className="error-main-title">{T.translate(`${I18N_PREFIX}.mainTitle`)}</h1>
      <h1>
        <strong>
          {isEmpty(entityType) || isEmpty(entityName) ? (
            T.translate(`${I18N_PREFIX}.genericMessage`)
          ) : (
            <span>{T.translate(`${I18N_PREFIX}.entityMessage`, { entityType, entityName })}</span>
          )}
        </strong>
      </h1>
      {children ? (
        children
      ) : (
        <div className="message-section">
          <h4>
            <strong>{T.translate(`${I18N_PREFIX}.subtitleMessage1`)}</strong>
          </h4>
          <div className="navigation-section">
            <div>
              {T.translate(`${I18N_PREFIX}.subtitleMessage2`)}
              <Link to={`/ns/${namespace}/control`}>
                {T.translate(`${I18N_PREFIX}.controlCenterLabel`)}
              </Link>
            </div>
            <div>
              {T.translate(`${I18N_PREFIX}.manageLabel`)}
              <a
                href={window.getHydratorUrl({
                  stateName: 'hydrator.list',
                  stateParams: {
                    namespace,
                  },
                })}
              >
                {T.translate(`${I18N_PREFIX}.pipelinesMessage`)}
              </a>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
Page404.defaultProps = {
  entityType: '',
  entityName: '',
};

Page404.propTypes = {
  location: PropTypes.object,
  entityType: PropTypes.string,
  entityName: PropTypes.string,
  children: PropTypes.node,
};
