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

import React from 'react';
import DeployedPipelineView from 'components/PipelineList/DeployedPipelineView';
import ResourceCenterButton from 'components/ResourceCenterButton';
import DraftPipelineView from 'components/PipelineList/DraftPipelineView';
import { Route, Switch, NavLink } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import Helmet from 'react-helmet';
import { Theme } from 'services/ThemeHelper';
import T from 'i18n-react';
import ErrorBoundary from 'components/ErrorBoundary';

import './PipelineList.scss';

const PREFIX = 'features.PipelineList';

const PipelineList: React.SFC = () => {
  const namespace = getCurrentNamespace();
  const basepath = `/ns/${namespace}/pipelines`;

  const productName = Theme.productName;
  const featureName = Theme.featureNames.pipelines;

  const pageTitle = `${productName} | ${featureName}`;

  return (
    <div className="pipeline-list-view">
      <Helmet title={pageTitle} />
      <h4 className="view-header" data-cy="pipeline-list-view-header">
        <NavLink exact to={basepath} className="option" activeClassName="active">
          {T.translate(`${PREFIX}.deployed`)}
        </NavLink>

        <span className="separator">|</span>

        <NavLink exact to={`${basepath}/drafts`} className="option" activeClassName="active">
          {T.translate(`${PREFIX}.draft`)}
        </NavLink>
      </h4>

      <ResourceCenterButton />

      <Switch>
        <Route
          exact
          path="/ns/:namespace/pipelines"
          component={() => {
            return (
              <ErrorBoundary>
                <DeployedPipelineView />
              </ErrorBoundary>
            );
          }}
        />
        <Route exact path="/ns/:namespace/pipelines/drafts" component={DraftPipelineView} />
      </Switch>
    </div>
  );
};

export default PipelineList;
