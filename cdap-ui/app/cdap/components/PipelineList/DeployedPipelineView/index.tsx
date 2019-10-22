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
import { reset } from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import PipelineCount from 'components/PipelineList/DeployedPipelineView/PipelineCount';
import SearchBox from 'components/PipelineList/DeployedPipelineView/SearchBox';
import Pagination from 'components/PipelineList/DeployedPipelineView/Pagination';
import { Provider } from 'react-redux';
import Store from 'components/PipelineList/DeployedPipelineView/store';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { gql } from 'apollo-boost';
import { useQuery } from '@apollo/react-hooks';

import './DeployedPipelineView.scss';
import { objectQuery } from 'services/helpers';

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
        totalRuns
      }
    }
  `;

  // on unmount
  React.useEffect(() => {
    return () => {
      reset();
    };
  });

  const { loading, error, data, refetch } = useQuery(QUERY, { errorPolicy: 'all' });

  if (loading) {
    return <LoadingSVGCentered />;
  }

  if (error) {
    // tslint:disable-next-line: no-console
    console.log('error', JSON.stringify(error, null, 2));
    const graphQLErrors = objectQuery(error, 'graphQLErrors') || [];
    const networkErrors = objectQuery(error, 'networkError') || [];

    let errors = graphQLErrors
      .concat(networkErrors)
      .map((err) => err.message)
      .join('\n');

    if (!errors || errors.length === 0) {
      const prefix = /^GraphQL error\:/;
      errors = error.message.replace(prefix, '').trim();
    }

    return <div className="pipeline-deployed-view error-container">{errors}</div>;
  }

  const pipelines = data.pipelines;

  return (
    <Provider store={Store}>
      <div className="pipeline-deployed-view pipeline-list-content">
        <div className="deployed-header">
          <PipelineCount pipelines={pipelines} pipelinesLoading={loading} />
          <SearchBox />
          <Pagination numPipelines={pipelines.length} />
        </div>

        <PipelineTable pipelines={pipelines} refetch={refetch} />
      </div>
    </Provider>
  );
};

export default DeployedPipeline;
