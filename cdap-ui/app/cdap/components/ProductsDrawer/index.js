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
import cookie from 'react-cookie';

export default class ProductsDrawer extends Component {
  constructor(props) {
    super(props);
    this.namespace;
    let products = [
      {
        label: T.translate('commons.cdap'),
        name: 'cdap',
        icon: 'icon-fist'
      },
      {
        label: T.translate('commons.hydrator'),
        name: 'hydrator',
        icon: 'icon-hydrator'
      },
      {
        label: T.translate('commons.tracker'),
        name: 'tracker',
        icon: 'icon-tracker'
      },
      {
        label: T.translate('commons.wrangler'),
        name: 'wrangler',
        icon: 'icon-DataWrangler'
      }
    ];
    let topProduct = head(products.filter(product => product.name === props.currentChoice));
    this.state = {
      productsDropdown: false,
      topProduct,
      products
    };

    this.toggle = this.toggle.bind(this);
  }
  componentWillMount(){
    this.updateNSLinks();
    NamespaceStore.subscribe(() => {
      this.updateNSLinks();
    });
  }
  updateNSLinks(){
    let NamespaceState = NamespaceStore.getState();
    this.namespace = NamespaceState.selectedNamespace || localStorage.getItem('DefaultNamespace');

    // If the user directly loads Management page without opening anything else.
    if (!this.namespace && Array.isArray(NamespaceState.namespaces) && NamespaceState.namespaces.length) {
      this.namespace = NamespaceState.namespaces[0].name;
    }
    let defaultUI = cookie.load('DEFAULT_UI');
    let products = this.state.products.map((product) => {

      switch(product.name) {
        case 'cdap' :
          product.link = `${defaultUI === 'OLD' ? '/old' : '/'}cdap/ns/${this.namespace}`;
          break;
        case 'hydrator' :
          product.link = `/hydrator/ns/${this.namespace}`;
          break;
        case 'tracker' :
          product.link = `/tracker/ns/${this.namespace}`;
          break;
        case 'wrangler' :
          product.link = `/wrangler/ns/${this.namespace}`;
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

    // The top item is always going to CDAP.
    let topItem = (
      <div
        className={classnames("top-product", this.state.products[0].name, {'open': this.state.productsDropdown})}
        onClick={this.toggle}
      >
        <span className={classnames("fa", this.state.products[0].icon)}></span>
        <span className="product-name">{this.state.products[0].label}</span>
      </div>
    );

    // If the products drawer is opened and if the current product that the user is in is not CDAP then make CDAP clickable.
    if(this.state.productsDropdown){
      if(this.state.topProduct.name !== this.state.products[0].name){
        topItem = (
          <a href={this.state.products[0].link}>
            {topItem}
          </a>
        );
      }
    } else {
      //else the product drawer is closed so just show the current product the user is in
      topItem = (
        <div
          className={classnames("top-product", this.state.topProduct.name, {'open': this.state.productsDropdown})}
          onClick={this.toggle}
        >
          <span className={classnames("fa", this.state.topProduct.icon)}></span>
          <span className="product-name">{this.state.topProduct.label}</span>
        </div>
      );
    }
    return (
      <Dropdown
        isOpen={this.state.productsDropdown}
        toggle={this.toggle}
      >
        {topItem}
        <DropdownMenu>
          {
            this.state
              .products
              .slice(1) // Just skip CDAP and show everything else in order as CDAP will always be the top item when the product drawer is open
              .map(product => {

                if(product.name === this.props.currentChoice){
                  return (
                    <div
                      className={classnames("dropdown-item product-link", product.name)}
                      key={shortid.generate()}
                      onClick={this.toggle}
                    >
                      <a
                        className={classnames("product-link", product.name)}
                      >
                        <span className={classnames("fa", product.icon)}></span>
                        <span>{product.label}</span>
                      </a>
                    </div>
                  );
                } else {
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
                }
              })
          }
        </DropdownMenu>
        {
          this.state.productsDropdown ?
            (
              <div
                className="products-backdrop"
                onClick={this.toggle}
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
