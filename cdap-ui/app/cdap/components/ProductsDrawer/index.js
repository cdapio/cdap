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
import { Dropdown, DropdownMenu } from 'reactstrap';
import T from 'i18n-react';
import classnames from 'classnames';
require('./ProductsDropdown.less');
import head from 'lodash/head';
import shortid from 'shortid';
import NamespaceStore from 'services/NamespaceStore';

export default class ProductsDrawer extends Component {
  constructor(props) {
    super(props);
    this.namespace;
    let products = [
      {
        link: '/cask-cdap/',
        label: T.translate('commons.cdap'),
        name: 'cdap',
        icon: 'icon-fist'
      },
      {
        link: '/cask-hydrator/',
        label: T.translate('commons.hydrator'),
        name: 'hydrator',
        icon: 'icon-hydrator'
      },
      {
        link: '/cask-tracker/',
        label: T.translate('commons.tracker'),
        name: 'tracker',
        icon: 'icon-tracker'
      }
    ];
    let currentChoice = head(products.filter(product => product.name === props.currentChoice));
    this.state = {
      productsDropdown: false,
      currentChoice,
      products
    };
  }
  componentWillMount(){
    this.updateNSLinks();
    NamespaceStore.subscribe(() => {
      this.updateNSLinks();
    });
  }
  updateNSLinks(){
    this.namespace = NamespaceStore.getState().selectedNamespace;
    let products = this.state.products.map((product) => {

      switch(product.name) {
        case 'cdap' :
          product.link = `/cask-cdap/ns/${this.namespace}`;
          break;
        case 'hydrator' :
          product.link = `/cask-hydrator/ns/${this.namespace}`;
          break;
        case 'tracker' :
          product.link = `/cask-tracker/ns/${this.namespace}`;
          break;
        default:
          break;
      }
      return product;
    });
    this.setState({
      products
    });
  }
  toggle() {
    this.setState({
      productsDropdown: !this.state.productsDropdown
    });
  }
  render() {
    return (
      <Dropdown
        isOpen={this.state.productsDropdown}
        toggle={this.toggle.bind(this)}
      >
        <div
          className={classnames("current-product", this.state.currentChoice.name, {'open': this.state.productsDropdown})}
          onClick={this.toggle.bind(this)}
        >
          <span className={classnames("fa", this.state.currentChoice.icon)}></span>
          <span className="product-name">{this.state.currentChoice.label}</span>
        </div>
        <DropdownMenu>
          {
            this.state
              .products
              .filter(product => product.name !== this.state.currentChoice.name)
              .map(product => {
                return (
                  <div
                    className="dropdown-item"
                    key={shortid.generate()}
                  >
                    <a
                      className={classnames("product-link", product.name)}
                      href={product.link}
                    >
                      <span className={classnames("fa", product.icon)}></span>
                      <span>{product.label}</span>
                    </a>
                  </div>
                );
              })
          }
        </DropdownMenu>
        {
          this.state.productsDropdown ?
            (
              <div
                className="products-backdrop"
                onClick={this.toggle.bind(this)}
              >
              </div>
            )
          :
            null
        }
      </Dropdown>
    );
  }
}

ProductsDrawer.propTypes = {
  currentChoice: PropTypes.string
};
