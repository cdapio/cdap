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
import { connect } from 'react-redux';
import {
  listSpannerInstances,
  listSpannerDatabases,
  listSpannerTables,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import { Link } from 'react-router-dom';
import { match } from 'react-router';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import T from 'i18n-react';

const PREFIX = `features.DataPrep.DataPrepBrowser.SpannerBrowser`;

interface IMatchParams {
  connectionId: string;
  instanceId: string;
}

interface ISpannerDatabaseListViewProps {
  databaseList: ISpannerDatabaseObject[];
  connectionId: string;
  instanceId: string;
  enableRouting: boolean;
  loading: boolean;
  match: match<IMatchParams>;
}

interface ISpannerDatabaseObject {
  name: string;
}

class SpannerDatabaseListView extends React.PureComponent<ISpannerDatabaseListViewProps> {
  public static defaultProps: Partial<ISpannerDatabaseListViewProps> = {
    enableRouting: true,
  };

  private clickHandler = (databaseId: string) => {
    if (this.props.enableRouting) {
      return;
    }
    const { connectionId, instanceId } = this.props;
    listSpannerTables(connectionId, instanceId, databaseId);
  };

  public componentDidMount() {
    if (!this.props.enableRouting) {
      return;
    }

    const { connectionId, instanceId } = this.props.match.params;

    listSpannerDatabases(connectionId, instanceId);
  }

  public render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    const { databaseList } = this.props;
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');
    const instanceId =
      this.props.instanceId || objectQuery(this.props, 'match', 'params', 'instanceId');

    if (!databaseList.length) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.emptyDatabaseList`, {
                connectionName: connectionId,
                instanceName: instanceId,
              })}
            </strong>
          </div>
        </div>
      );
    }

    const Tag = this.props.enableRouting ? Link : 'span';
    const namespace = getCurrentNamespace();
    const path = `/ns/${namespace}/connections/spanner/${connectionId}`;

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          <div className="path">
            <Tag
              to={path}
              className="instance-path"
              onClick={listSpannerInstances.bind(null, connectionId)}
            >
              {T.translate(`${PREFIX}.instances`)}
            </Tag>
            <span> / </span>
            <span>{instanceId}</span>
          </div>
          <div>{T.translate(`${PREFIX}.databaseCount`, { context: databaseList.length })}</div>
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-12">{T.translate(`${PREFIX}.name`)}</div>
            </div>
          </div>

          <div className="table-body">
            {databaseList.map((database: ISpannerDatabaseObject) => {
              const ElemTag = this.props.enableRouting ? Link : 'div';
              const instanceUrl = `/ns/${namespace}/connections/spanner/${connectionId}/instances/${instanceId}`;
              const databaseUrl = `${instanceUrl}/databases/${database.name}`;

              return (
                <ElemTag
                  key={database.name}
                  to={databaseUrl}
                  onClick={this.clickHandler.bind(null, database.name)}
                >
                  <div className="row content-row">
                    <div className="col-12">
                      <IconSVG name="icon-database" />
                      {database.name}
                    </div>
                  </div>
                </ElemTag>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state): Partial<ISpannerDatabaseListViewProps> => {
  return {
    instanceId: state.spanner.instanceId,
    databaseList: state.spanner.databaseList,
    connectionId: state.spanner.connectionId,
    loading: state.spanner.loading,
  };
};

const SpannerDatabaseList = connect(mapStateToProps)(SpannerDatabaseListView);

export default SpannerDatabaseList;
