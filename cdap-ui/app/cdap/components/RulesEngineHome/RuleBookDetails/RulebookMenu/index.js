/*
 * Copyright © 2017 Cask Data, Inc.
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
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import MyRulesEngine from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {setActiveRulebook, getRuleBooks, setError} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import AddRulesEngineToPipelineModal from 'components/RulesEngineHome/RuleBookDetails/RulebookMenu/AddRulesEngineToPipelineModal';
import T from 'i18n-react';

require('./RulebookMenu.scss');
const PREFIX = 'features.RulesEngine.RulebookMenu';

export default class RulebookMenu extends Component {
  static propTypes = {
    mode: PropTypes.string,
    rulebookid: PropTypes.string,
    embedded: PropTypes.bool
  };

  state = {
    openAddToPipelineModal: false,
    downloadFileName: false,
    anchorLink: false
  };

  deleteWorkbook = () => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngine
      .deleteRulebook({
        namespace,
        rulebookid: this.props.rulebookid
      })
      .subscribe(
        () => {
          setActiveRulebook();
          getRuleBooks();
        },
        setError
      );
  };

  downloadRulebook = () => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngine
      .getRulebook({
        namespace,
        rulebookid: this.props.rulebookid
      })
      .subscribe(
        (res) => {
          let rulebook = res.values[0];
          var blob = new Blob([rulebook]);
          this.setState({
            downloadUrl: URL.createObjectURL(blob),
            downloadFileName: this.props.rulebookid
          }, () => {
            this.anchorLink.click();
            this.setState({
              downloadFileName: false,
              downloadUrl: false,
              anchorLink: false
            });
          });
        }
      );
  };

  toggleRulesEngineToPipelineModal = () => {
    this.setState({
      openAddToPipelineModal: !this.state.openAddToPipelineModal
    });
  };

  menu = [
    {
      label: T.translate(`${PREFIX}.download`),
      onClick: this.downloadRulebook,
      iconName: 'icon-download'
    },
    {
      label: T.translate(`${PREFIX}.createPipeline`),
      onClick: this.toggleRulesEngineToPipelineModal,
      iconName: 'icon-pipelines',
      skipInPipelines: true
    },
    {
      label: 'divider'
    },
    {
      label: T.translate(`${PREFIX}.delete`),
      onClick: this.deleteWorkbook,
      iconName: 'icon-trash'
    }
  ];

  render() {
    const renderMenuItem = (menu) => {
      let {label, iconName} = menu;
      return (
        <div>
          {
            iconName ?
              <IconSVG name={iconName} />
            :
              null
          }
          <span>{label}</span>
        </div>
      );
    };
    let menuItems = this.menu;
    if (this.props.embedded) {
      menuItems = menuItems.filter(item => !item.skipInPipelines);
    }
    return (
      <div className="rule-book-menu">
        <UncontrolledDropdown>
          <DropdownToggle>
            <IconSVG name="icon-bars" />
            <IconSVG name="icon-caret-down" />
          </DropdownToggle>
          <DropdownMenu right>
            {
              menuItems.map((menu) => {
                if (menu.label === 'divider') {
                  return <hr />;
                }
                return (
                  <DropdownItem
                    title={menu.label}
                    onClick={menu.onClick}
                  >
                    {renderMenuItem(menu)}
                  </DropdownItem>
                );
              })
            }
          </DropdownMenu>
        </UncontrolledDropdown>
        <AddRulesEngineToPipelineModal
          isOpen={this.state.openAddToPipelineModal}
          onClose={this.toggleRulesEngineToPipelineModal}
          rulebookid={this.props.rulebookid}
        />
        {
          this.state.downloadUrl ?
            <a
              href={this.state.downloadUrl}
              download={this.state.downloadFileName}
              id="download-anchor-link"
              ref={(ref) => this.anchorLink = ref}
            ></a>
          :
            null
        }
      </div>
    );
  }
}
