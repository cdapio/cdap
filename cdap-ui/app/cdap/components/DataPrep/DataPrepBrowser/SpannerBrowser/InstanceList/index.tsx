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
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import { Link } from 'react-router-dom';
import { match } from 'react-router';
import { getCurrentNamespace } from 'services/NamespaceStore';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';

const PREFIX = `features.DataPrep.DataPrepBrowser.SpannerBrowser`;

interface IMatchParams {
  connectionId: string;
}

interface ISpannerInstanceListViewProps {
  instanceList: ISpannerInstanceObject[];
  connectionId: string;
  enableRouting: boolean;
  loading: boolean;
  match: match<IMatchParams>;
}

interface ISpannerInstanceObject {
  name: string;
}

class SpannerInstanceListView extends React.PureComponent<ISpannerInstanceListViewProps> {
  public static defaultProps: Partial<ISpannerInstanceListViewProps> = {
    enableRouting: true,
  };

  public state = {
    connectionId:
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId'),
  };

  private clickHandler = (instanceId: string) => {
    if (this.props.enableRouting) {
      return;
    }
    listSpannerDatabases(this.props.connectionId, instanceId);
  };

  public componentDidMount() {
    if (!this.props.enableRouting) {
      return;
    }

    const { connectionId } = this.props.match.params;

    listSpannerInstances(connectionId);
  }

  public componentDidUpdate() {
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');

    if (connectionId !== this.state.connectionId) {
      listSpannerInstances(connectionId);
      this.setState({
        connectionId,
      });
    }
  }

  public render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    const { instanceList } = this.props;
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');

    if (!instanceList.length) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.emptyInstanceList`, {
                connectionName: connectionId,
              })}
            </strong>
          </div>
        </div>
      );
    }

    const namespace = getCurrentNamespace();

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          {T.translate(`${PREFIX}.instanceCount`, { context: instanceList.length })}
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-12">{T.translate(`${PREFIX}.name`)}</div>
            </div>
          </div>

          <div className="table-body">
            {instanceList.map((instance: ISpannerInstanceObject) => {
              const Tag = this.props.enableRouting ? Link : 'div';
              const path = `/ns/${namespace}/connections/spanner/${connectionId}/instances/${
                instance.name
              }`;

              return (
                <Tag
                  key={instance.name}
                  to={path}
                  onClick={this.clickHandler.bind(null, instance.name)}
                >
                  <div className="row content-row">
                    <div className="col-12">
                      <IconSVG name="icon-database" className="instance-icon" />
                      {instance.name}
                    </div>
                  </div>
                </Tag>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state): Partial<ISpannerInstanceListViewProps> => {
  return {
    instanceList: state.spanner.instanceList,
    connectionId: state.spanner.connectionId,
    loading: state.spanner.loading,
  };
};

const SpannerInstanceList = connect(mapStateToProps)(SpannerInstanceListView);

export default SpannerInstanceList;
