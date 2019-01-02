import React from 'react';
import NameValueList from '../NameValueList';
import cloneDeep from 'lodash/cloneDeep';

require('./Configurator.scss');

class Configurator extends React.Component {
  configPropList;
  constructor(props) {
    super(props);
    this.configPropList = [];
  }

  componentDidMount() {
    this.configPropList = this.props.availableConfigurations.map((config)=> {
      return {
        name: config.paramName,
        value: '',
        dataType: config.dataType,
        isCollection: config.isCollection,
        toolTip: "Type: " + (config.isCollection ? ("Collection of "+ config.dataType): config.dataType)
      };
    });
    this.props.updateConfigurationList(this.configPropList);
  }


  addConfiguration(nameValue) {
    this.configPropList = cloneDeep(this.props.configurationList);
    this.configPropList.push(nameValue);
    this.props.updateConfigurationList(this.configPropList);
  }

  updateConfiguration(index, nameValue) {
    this.configPropList[index] = nameValue;
    this.props.updateConfigurationList(this.configPropList);
  }

  render() {
    return (
      <div className = 'configuration-step-container'>
        <NameValueList dataProvider = {this.props.configurationList}
         updateNameValue = {this.updateConfiguration.bind(this)}
         addNameValue = {this.addConfiguration.bind(this)}/>
      </div>
    );
  }
}
export default Configurator;