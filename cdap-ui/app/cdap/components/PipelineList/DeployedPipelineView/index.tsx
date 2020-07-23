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
import PipelineTable from 'components/PipelineList/DeployedPipelineView/PipelineTable';
import {
  reset,
  setFilteredPipelines,
} from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import PipelineCount from 'components/PipelineList/DeployedPipelineView/PipelineCount';
import SearchBox from 'components/PipelineList/DeployedPipelineView/SearchBox';
import Pagination from 'components/PipelineList/DeployedPipelineView/Pagination';
import { Provider } from 'react-redux';
import Store from 'components/PipelineList/DeployedPipelineView/store';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { gql } from 'apollo-boost';
import { useQuery } from '@apollo/react-hooks';
import If from 'components/If';
import { categorizeGraphQlErrors } from 'services/helpers';
import ErrorBanner from 'components/ErrorBanner';
import T from 'i18n-react';

import './DeployedPipelineView.scss';
const I18N_PREFIX = 'features.PipelineList.DeployedPipelineView';

const DeployedPipeline: React.FC = () => {
  const QUERY = gql`
    {
      pipelines(namespace: "${getCurrentNamespace()}") {
        name,
        artifact {
          name
        },
        runs {
          status,
          starting
        },
        totalRuns,
        nextRuntime {
          id,
          time
        }
      }
    }
  `;

  // on unmount
  React.useEffect(() => {
    return () => {
      reset();
    };
  }, []);
  let bannerMessage = '';
  const { loading, error, data, refetch, networkStatus } = useQuery(QUERY, {
    errorPolicy: 'all',
    notifyOnNetworkStatusChange: true,
  });

  if (loading || networkStatus === 4) {
    return <LoadingSVGCentered />;
  }

  if (error) {
    const errorMap = categorizeGraphQlErrors(error);
    // Errors thrown here will be caught by error boundary
    // and will show error to the user within pipeline list view

    // Each error type could have multiple error messages, we're using the first one available
    if (errorMap.hasOwnProperty('pipelines')) {
      throw new Error(errorMap.pipelines[0]);
    } else if (errorMap.hasOwnProperty('network')) {
      throw new Error(errorMap.network[0]);
    } else if (errorMap.hasOwnProperty('generic')) {
      throw new Error(errorMap.generic[0]);
    } else {
      if (Object.keys(errorMap).length > 1) {
        // If multiple services are down
        const message = T.translate(`${I18N_PREFIX}.graphQLMultipleServicesDown`).toString();
        throw new Error(message);
      } else {
        // Pick one of the leftover errors to show in the banner;
        bannerMessage = Object.values(errorMap)[0][0];
      }
    }
  }

  setFilteredPipelines(data.pipelines);

  return (
    <Provider store={Store}>
      <div className="pipeline-deployed-view pipeline-list-content">
        <div className="deployed-header">
          <PipelineCount pipelinesLoading={loading} />
          <SearchBox />
          <Pagination />
        </div>

        <If condition={!!error && !!bannerMessage}>
          <ErrorBanner error={bannerMessage} />
        </If>
        <PipelineTable refetch={refetch} />
      </div>
    </Provider>
  );
};

export default DeployedPipeline;
