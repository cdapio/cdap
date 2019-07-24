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
  fetchPipelineList,
  reset,
} from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import PipelineCount from 'components/PipelineList/DeployedPipelineView/PipelineCount';
import SearchBox from 'components/PipelineList/DeployedPipelineView/SearchBox';
import Pagination from 'components/PipelineList/DeployedPipelineView/Pagination';
import { Provider } from 'react-redux';
import Store from 'components/PipelineList/DeployedPipelineView/store';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { getCurrentNamespace } from 'services/NamespaceStore';

import './DeployedPipelineView.scss';

import { Query } from 'react-apollo';
import { gql } from 'apollo-boost';

export default class DeployedPipelineView extends React.PureComponent {
  public componentDidMount() {
    fetchPipelineList();
  }

  public componentWillUnmount() {
    reset();
  }

  public render() {
    return <DeployedPipelinesView />;
  }
}

const currentNamespace = getCurrentNamespace();

const DeployedPipelinesView = () => (
  <Query
    query={gql`
      {
        applications(namespace: "${currentNamespace}", artifactName: "cdap-data-pipeline,cdap-data-streams") {
          name
          artifact {
            name
          }
          applicationDetail {
            programs {
              name
              runs {
                status
                starting
              }
              totalRuns
              ... on Workflow {
                schedules {
                  name
                  status
                  nextRuntimes
                }
              }
            }
          }
        }
      }
    `}
  >
    {({ loading, error, data, refetch }) => {
      if (loading) {
        return <LoadingSVGCentered />;
      }
      if (error) {
        return <p>Error! {error.message}</p>;
      }

      const pipelines = data.applications;

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
    }}
  </Query>
);
