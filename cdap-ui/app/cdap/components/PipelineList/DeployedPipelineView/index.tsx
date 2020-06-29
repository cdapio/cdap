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
import Page500 from 'components/500';
import ErrorBanner from 'components/ErrorBanner';
import If from 'components/If';

import './DeployedPipelineView.scss';
import { objectQuery } from 'services/helpers';

// For errors of this type coming from graphQl, we show a 500 error instead of a banner.
const HIGH_PRIORITY_ERRORS = new Set(['pipelinesList']);

interface IErrorForDisplay {
  message: string;
  stack: object;
  errorOrigin: string;
}

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

  const { loading, error, data, refetch, networkStatus } = useQuery(QUERY, {
    errorPolicy: 'all',
    notifyOnNetworkStatusChange: true,
  });

  if (loading || networkStatus === 4) {
    return <LoadingSVGCentered />;
  }

  let hasHighPriorityError = false;
  let errorForDisplay: IErrorForDisplay;
  const errorsByOrigin = {};
  if (error) {
    // tslint:disable-next-line: no-console
    console.log('error', JSON.stringify(error, null, 2));
    const graphQLErrors = objectQuery(error, 'graphQLErrors') || [];
    const networkErrors = objectQuery(error, 'networkError') || [];

    const allErrors: IErrorForDisplay[] = graphQLErrors.concat(networkErrors).map((err) => {
      const stack = objectQuery(err, 'extensions', 'exception', 'stacktrace');
      const errorOrigin = objectQuery(err, 'extensions', 'exception', 'errorOrigin');

      return {
        message: err.message,
        stack,
        errorOrigin,
      };
    });

    if (!allErrors || allErrors.length === 0) {
      const prefix = /^GraphQL error\:/;
      errorForDisplay = {
        message: error.message.replace(prefix, '').trim(),
        stack: {},
        errorOrigin: 'generic',
      };
    } else {
      // Categorizing errors by their origin.
      allErrors.forEach((eachError: IErrorForDisplay) => {
        if (!eachError.errorOrigin) {
          eachError.errorOrigin = 'generic';
        }
        if (errorsByOrigin.hasOwnProperty(eachError.errorOrigin)) {
          errorsByOrigin[eachError.errorOrigin].push(eachError);
        } else {
          errorsByOrigin[eachError.errorOrigin] = [eachError];
        }
      });

      const errorTypes = Object.keys(errorsByOrigin);
      // Finding the first high priority error for display.
      const hpError = errorTypes.find((errorType) => HIGH_PRIORITY_ERRORS.has(errorType));

      if (hpError) {
        // If there is a high priorty error, pick that one.
        errorForDisplay = errorsByOrigin[hpError][0];
        hasHighPriorityError = true;
      } else if (errorTypes.length > 1) {
        // When more than one service is down i.e different error types, consider it high priority.
        hasHighPriorityError = true;
        errorForDisplay = allErrors[0];
        errorForDisplay.message =
          'Multiple services ran into issues when trying to retrieve list of pipelines. Please try again later.';
      } else {
        // Pick the first error to display to the user.
        errorForDisplay = allErrors[0];
      }
    }

    if (hasHighPriorityError) {
      return (
        <div className="pipeline-deployed-view">
          <Page500 message={errorForDisplay.message} stack={errorForDisplay.stack} />
        </div>
      );
    }
  }

  function generateErrorBannerMessage() {
    const errorTypesCount = Object.keys(errorsByOrigin).length;
    return errorForDisplay
      ? `${errorForDisplay.message}${
          errorTypesCount > 1
            ? ` and ${errorTypesCount - 1} other error${
                errorTypesCount - 1 > 1 ? 's' : ''
              } occured.`
            : ''
        }`
      : 'Error occured while trying to retrieve information required for list of pipelines.';
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
        <If condition={!!error && !hasHighPriorityError}>
          <ErrorBanner error={generateErrorBannerMessage()} />
        </If>
        <PipelineTable refetch={refetch} />
      </div>
    </Provider>
  );
};

export default DeployedPipeline;
