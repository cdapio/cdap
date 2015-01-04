window.PKG = {
  name: 'cdap-ui'
};

angular.module(PKG.name + ".config", []).constant("MY_CONFIG", {
  "autorization": "respect my authoritah",
  cdap: {
    routerServerUrl: "test"
  }
});
