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

import * as React from 'react';
import { Route, Switch } from 'react-router-dom';
import Helmet from 'react-helmet';
import List from 'components/Transfers/List';
import T from 'i18n-react';
import { Theme } from 'services/ThemeHelper';
import Create from 'components/Transfers/Create';
import Detail from './Detail';
import { MyDeltaApi } from 'api/delta';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import enableSystemApp from 'services/ServiceEnablerUtilities';

const basepath = '/ns/:namespace/transfers';

const Transfers: React.SFC = () => {
  const [backendUp, setBackendUp] = React.useState(false);
  const [loading, setLoading] = React.useState(true);

  // tslint:disable:no-console
  React.useEffect(() => {
    // check if backend is up
    MyDeltaApi.ping().subscribe(
      () => {
        setBackendUp(true);
        setLoading(false);
      },
      () => {
        console.log('DeltaForce service not started. Starting.....');
        enableSystemApp({
          shouldStopService: false,
          artifactName: 'delta-service',
          api: MyDeltaApi,
          i18nPrefix: 'features.Transfers',
          featureName: 'Replicator',
          MIN_VERSION: '0.0.0',
        }).subscribe(
          () => {
            console.log('DeltaForce started');
            setBackendUp(true);
            setLoading(false);
          },
          (err) => {
            console.log('Error', err);
          }
        );
      }
    );
  }, []);
  // tslint:enable:no-console

  if (loading) {
    return <LoadingSVGCentered />;
  }

  return (
    <div style={{ height: '100%' }}>
      <Helmet
        title={T.translate('features.Transfers.pageTitle', {
          productName: Theme.productName,
          featureName: Theme.featureNames.transfers,
        })}
      />
      <If condition={backendUp}>
        <Switch>
          <Route exact path={basepath} component={List} />
          <Route exact path={`${basepath}/create`} component={Create} />
          <Route exact path={`${basepath}/create/:id`} component={Create} />
          <Route exact path={`${basepath}/details/:id`} component={Detail} />
          <Route exact path={`${basepath}/edit/:id`} component={Create} />
        </Switch>
      </If>
    </div>
  );
};

export default Transfers;
