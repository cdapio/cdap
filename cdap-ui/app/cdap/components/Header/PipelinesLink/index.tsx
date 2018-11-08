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
import * as React from 'react';
import { Theme } from 'services/ThemeHelper';
import { withContext } from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
require('./PipelinesLink.scss');

declare global {
  /* tslint:disable:interface-name */
  interface Window {
    getHydratorUrl: ({}) => string;
  }
}

interface IPipelinesLinkProps {
  context: {
    namespace: string;
  };
}

const PipelinesLink: React.SFC<IPipelinesLinkProps> = ({ context }) => {
  if (Theme.showPipelines === false) {
    return null;
  }
  const featureName = Theme.featureNames.pipelines;
  const { namespace } = context;
  const pipelinesListUrl = window.getHydratorUrl({
    stateName: 'hydrator.list',
    stateParams: {
      namespace,
      page: 1,
      sortBy: '_stats.lastStartTime',
    },
  });
  const isPipelinesViewActive = location.pathname.indexOf('/pipelines/') !== -1;

  return (
    <li
      id="navbar-pipelines"
      className={classnames({
        active: isPipelinesViewActive,
      })}
    >
      <a href={pipelinesListUrl}>{featureName}</a>
    </li>
  );
};

const PipelinesLinkWithContext = withContext(PipelinesLink);

export default PipelinesLinkWithContext;
