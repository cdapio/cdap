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

export default class ExploreAction extends Component {
  constructor(props) {
    super(props);
    this.state = {
      disabled: true,
      showModal: false,
      tooltipOpen: false
    };
    this.subscription = null;
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
  }
  toggleModal() {
    this.setState({
      showModal: !this.state.showModal
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
      let match = explorableTables.filter(db => db.table === `${type}_${entityId.toLowerCase()}`);
      if (match.length) {
        this.setState({
          disabled: false
        });
      }
    };
    this.subscription = ExploreTablesStore.subscribe(updateDisabledProp.bind(this));
    updateDisabledProp();
  }
  componentWillUnmount() {
    this.subscription();
  }
  render() {
    let tooltipID = `${this.props.entity.uniqueId}-explore`;
    return (
      <span>
        <FastActionButton
          icon="fa fa-eye"
          action={this.toggleModal}
          disabled={this.state.disabled}
          id={tooltipID}
        />
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
              entity={this.props.entity}
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
  opened: PropTypes.bool
};
