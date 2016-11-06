import React from 'react';
const OverviewTabConfig = {
  defaultTab: 1,
  layout: 'horizontal',
  tabs: [
    {
      id: 1,
      icon: '',
      name: 'Main',
      content: <div> Main Tab Content</div>
    },
    {
      id: 3,
      icon: '',
      name: 'Dataset',
      content: <div> Dataset Tab Content </div>
    }
  ]
};
export default OverviewTabConfig;
