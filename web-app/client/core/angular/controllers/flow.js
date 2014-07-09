'use strict';

define(['helpers'], function (helpers) {

  /* Items */

  var Ctrl = ['$scope', '$interval', '$routeParams', 'dataFactory', 'POLLING_INTERVAL',
    function($scope, $interval, $routeParams, dataFactory, POLLING_INTERVAL) {

    /**
     * @type {Flow}
     */
    $scope.flow = {};

    var statusEndpoints = [];
    var intervals = [];

    var appId = $routeParams.appId;
    var flowId = $routeParams.flowId;
    var workflowId = $routeParams.workflowId;


    dataFactory.getFlowByAppNameAndId(appId, flowId, function (flow) {
      $scope.flow = flow;
    });

    $scope.getStatusEndpoint = function (entity) {
      return helpers.getStatusEndpoint(entity);
    };

    $scope.$on("$destroy", function(){

      if (typeof intervals !== 'undefined') {
        helpers.cancelAllIntervals($interval, intervals);
      }
    });


  }];

  return Ctrl;

});