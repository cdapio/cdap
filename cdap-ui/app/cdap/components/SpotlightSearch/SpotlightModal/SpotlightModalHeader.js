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
import T from 'i18n-react';
import {
  ModalHeader,
  Dropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem
} from 'reactstrap';

require('./SpotlightModal.less');

 export default class SpotlightModalHeader extends Component {
   constructor(props){
     super(props);
     this.state = {
       isDropdownExpanded : false
     };

     this.toggleExpansion = this.toggleExpansion.bind(this);
   }

   toggleExpansion() {
     console.log('toggle initiated!');
     this.setState({
       isDropdownExpanded : !this.state.isDropdownExpanded
     });
   }

   render() {

     let pageArray = Array.from(Array(this.props.numPages).keys()).map( n => n + 1 );

     return(
       <ModalHeader>
         <span className="pull-left">
           {

             T.translate('features.SpotlightSearch.SpotlightModal.headerSearchResults', {
               query: this.props.query
             })
           }
         </span>
         <div
           className="close-section pull-right"
         >
           <span className="search-results-total">
             {
               T.translate('features.SpotlightSearch.SpotlightModal.numResults', {
                 total: this.props.total
               })
             }
           </span>
           <span>
           <Dropdown
             isOpen={this.state.isDropdownExpanded}
             toggle={this.toggleExpansion}
           >
             <DropdownToggle tag="div">
               <span>Page: {this.props.currentPage}</span>
               <span className="fa fa-caret-down pull-right"></span>
             </DropdownToggle>
             <DropdownMenu>
               {
                 pageArray.map((page, index) => {
                   return (
                     <DropdownItem
                       key={index}
                       onClick={this.props.handleSearch.bind(this, page)}
                     >
                       {page}
                     </DropdownItem>
                   );
                 })
               }
             </DropdownMenu>
           </Dropdown>
           </span>
           <span
             className="fa fa-times"
             onClick={this.props.toggle}
           />
         </div>
       </ModalHeader>
     );

   }
 }

 SpotlightModalHeader.propTypes = {
   toggle: PropTypes.func,
   handleSearch: PropTypes.func,
   currentPage: PropTypes.number,
   query: PropTypes.string,
   numPages: PropTypes.number,
   total: PropTypes.number
 };
