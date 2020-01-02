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
import NamespaceStore, { getValidNamespace } from 'services/NamespaceStore';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
import If from 'components/If';

require('./404.scss');

const I18N_PREFIX = 'features.Page404';

export default function Page404({ entityName, entityType, children, message }) {
  const [validNs, setvalidNs] = React.useState('');

  React.useEffect(() => {
    const { selectedNamespace } = NamespaceStore.getState();
    async function getValidNs() {
      const validNs = await getValidNamespace(selectedNamespace);
      setvalidNs(validNs);
    }
    getValidNs();
  }, []);

  return (
    <div className="page-not-found">
      <h1 className="error-main-title">{T.translate(`${I18N_PREFIX}.mainTitle`)}</h1>
      <h1>
        <strong>
          <If condition={typeof message === 'string'}>
            <span data-cy="page-404-error-msg">{message}</span>
          </If>
          <If condition={!message}>
            <span data-cy="page-404-default-msg">
              {isEmpty(entityType) || isEmpty(entityName)
                ? T.translate(`${I18N_PREFIX}.genericMessage`)
                : T.translate(`${I18N_PREFIX}.entityMessage`, { entityType, entityName })}
            </span>
          </If>
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
              <a href={`/cdap/ns/${validNs}/`}>{T.translate(`${I18N_PREFIX}.homePageLabel`)}</a>
            </div>
            <div>
              {T.translate(`${I18N_PREFIX}.manageLabel`)}
              <a
                href={window.getHydratorUrl({
                  stateName: 'hydrator.list',
                  stateParams: {
                    namespace: validNs,
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
  message: PropTypes.string,
};
