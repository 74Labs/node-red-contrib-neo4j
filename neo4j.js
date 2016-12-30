module.exports = function(RED) {

    "use strict";

    var neo4j = require('neo4j-driver').v1;

    function Neo4jConnectionNode(config) {
        RED.nodes.createNode(this, config);
        var driver = neo4j.driver("bolt://" + config.url, neo4j.auth.basic(config.username, config.password));
        this.createSession = function() {
          return driver.session();
        };
    }

    function Neo4jNode(config) {

        RED.nodes.createNode(this, config);
        var node = this;
        node.config = RED.nodes.getNode(config.neo4j);
        node.cypher = config.cypher;
        node.streaming = config.streaming;

        node.on('input', function(msg) {

            node.status({
                fill: 'blue',
                shape: 'dot',
                text: 'pending'
            });

            var session = node.config.createSession();
            var s = session.run(node.cypher, msg.payload);

            if(node.streaming) {
              s.subscribe({
                onNext: function(record) {
                  node.send(record);
                },
                onCompleted: function() {
                  session.close();
                  node.status({
                      fill: 'yellow',
                      shape: 'dot',
                      text: 'success'
                  });
                },
                onError: function(err) {
                  node.status({
                      fill: 'red',
                      shape: 'dot',
                      text: 'error'
                  });
                  node.error(err);
                  return;
                }
            });
          } else {

              s.then(function(result) {
                node.send(result.records);
                session.close();
                node.status({
                    fill: 'yellow',
                    shape: 'dot',
                    text: 'success'
                });
              })
              .catch(function(err) {
                node.status({
                    fill: 'red',
                    shape: 'dot',
                    text: 'error'
                });
                node.error(err);
                return;
              });
          }

        });
    }

    RED.nodes.registerType("neo4j-conn",Neo4jConnectionNode);
    RED.nodes.registerType("neo4", Neo4jNode);

};
