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

import React, {Component, PropTypes} from 'react';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import FastActionButton from '../FastActionButton';
import {Tooltip} from 'reactstrap';
import ExploreModal from 'components/FastAction/ExploreAction/ExploreModal';
import T from 'i18n-react';
import {objectQuery} from 'services/helpers';
import myExploreApi from 'api/explore';
import NamespaceStore from 'services/NamespaceStore';
import classnames from 'classnames';
require('./ExploreAction.scss');

export default class ExploreAction extends Component {
  constructor(props) {
    super(props);
    this.state = {
      disabled: true,
      showModal: false,
      tooltipOpen: false,
      entity: this.props.entity || {},
      runningQueries: null,
      showRunningQueriesDoneLabel: false
    };
    this.subscription = null;
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
  }
  toggleModal(runningQueries) {
    let showRunningQueriesDoneLabel = false;
    if (runningQueries && this.state.showModal) {
      showRunningQueriesDoneLabel = true;
    }
    this.setState({
      showModal: !this.state.showModal,
      showRunningQueriesDoneLabel,
      runningQueries: !showRunningQueriesDoneLabel ? null : runningQueries,
    });
  }
  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }
  componentWillMount() {
    if (this.props.opened) {
      this.setState({showModal: true});
    }
  }
  componentDidMount() {
    const updateDisabledProp = () => {
      let {tables: explorableTables} = ExploreTablesStore.getState();
      let entityId = this.props.entity.id.replace(/[\.\-]/g, '_');
      let type = this.props.entity.type === 'datasetinstance' ? 'dataset' : this.props.entity.type;
      let match = explorableTables.filter(db => {
        return db.table === `${type}_${entityId.toLowerCase()}` || db.table === entityId.toLowerCase();
      });
      if (match.length) {
        this.setState({
          entity: Object.assign({}, this.props.entity, {
            tableName: match[0].table,
            databaseName: match[0].database
          }),
          disabled: false
        });
      }
    };
    this.subscription = ExploreTablesStore.subscribe(updateDisabledProp.bind(this));
    updateDisabledProp();
    if (objectQuery(this.props.argsToAction, 'showQueriesCount')) {
      let namespace = NamespaceStore.getState().selectedNamespace;
      this.explroeQueriesSubscription$ = myExploreApi
        .fetchQueries({namespace})
        .subscribe((res) => {
          let runningQueries = res
            .filter(q => {
              let tablename = `${this.state.entity.databaseName}.${this.state.entity.tableName}`;
              return (
                (
                  q.statement.indexOf(tablename) !== -1 || q.statement.indexOf(this.props.entity.id) !== -1
                )
                &&
                q.status === 'RUNNING'
              );
            }).length;
          if (runningQueries) {
            this.setState({ runningQueries });
            return;
          }
          if (this.state.runningQueries) {
            this.setState({
              runningQueries: T.translate('features.FastAction.doneLabel')
            });
          }
        });
    }
  }
  componentWillUnmount() {
    this.subscription();
    if (this.explroeQueriesSubscription$) {
      this.explroeQueriesSubscription$.dispose();
    }
  }
  render() {
    let tooltipID = `${this.props.entity.uniqueId}-explore`;
    let showRunningQueriesNotification = this.state.showRunningQueriesDoneLabel && this.state.runningQueries && objectQuery(this.props.argsToAction, 'showQueriesCount');
    return (
      <span className={classnames("btn btn-secondary btn-sm", {'fast-action-with-popover': showRunningQueriesNotification})}>
        <FastActionButton
          icon="fa fa-eye"
          action={this.toggleModal}
          disabled={this.state.disabled}
          id={tooltipID}
        />
        {
          showRunningQueriesNotification ?
            <span className="fast-action-popover-container">{this.state.runningQueries}</span>
          :
            null
        }
        <Tooltip
          placement="top"
          className="fast-action-tooltip"
          isOpen={this.state.tooltipOpen}
          target={tooltipID}
          toggle={this.toggleTooltip}
          delay={0}
        >
          {T.translate('features.FastAction.exploreLabel')}
        </Tooltip>
        {
          this.state.showModal ?
            <ExploreModal
              isOpen={this.state.showModal}
              onClose={this.toggleModal}
              entity={this.state.entity}
            />
          :
            null
        }
      </span>
    );
  }
}
ExploreAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    version: PropTypes.string,
    uniqueId: PropTypes.string,
    scope: PropTypes.oneOf(['SYSTEM', 'USER']),
    type: PropTypes.oneOf(['application', 'artifact', 'datasetinstance', 'stream']).isRequired,
  }),
  opened: PropTypes.bool,
  argsToAction: PropTypes.object
};
