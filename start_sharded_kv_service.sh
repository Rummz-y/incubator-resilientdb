#!/bin/bash
pkill kv_service
sleep 2

SERVER_PATH=./bazel-bin/service/kv/kv_service
WORK_PATH=$PWD
CERT_PATH=${WORK_PATH}/service/tools/data/cert/
GLOBAL_CONFIG=service/tools/config/shards/global.config

bazel build //service/kv:kv_service --define enable_leveldb=True $@

# Shard 1 (nodes 1-4)
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node1.key.pri  $CERT_PATH/cert_1.cert  8090 > shard1_node1.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node2.key.pri  $CERT_PATH/cert_2.cert  8091 > shard1_node2.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node3.key.pri  $CERT_PATH/cert_3.cert  8092 > shard1_node3.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node4.key.pri  $CERT_PATH/cert_4.cert  8093 > shard1_node4.log &

# Shard 2 (nodes 5-8)
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node5.key.pri  $CERT_PATH/cert_5.cert  8094 > shard2_node5.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node6.key.pri  $CERT_PATH/cert_6.cert  8095 > shard2_node6.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node7.key.pri  $CERT_PATH/cert_7.cert  8096 > shard2_node7.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node8.key.pri  $CERT_PATH/cert_8.cert  8097 > shard2_node8.log &

# Shard 3 (nodes 9-12)
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node9.key.pri  $CERT_PATH/cert_9.cert  8098 > shard3_node9.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node10.key.pri $CERT_PATH/cert_10.cert 8099 > shard3_node10.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node11.key.pri $CERT_PATH/cert_11.cert 8100 > shard3_node11.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node12.key.pri $CERT_PATH/cert_12.cert 8101 > shard3_node12.log &

# Shard 4 (nodes 13-16)
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node13.key.pri $CERT_PATH/cert_13.cert 8102 > shard4_node13.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node14.key.pri $CERT_PATH/cert_14.cert 8103 > shard4_node14.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node15.key.pri $CERT_PATH/cert_15.cert 8104 > shard4_node15.log &
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node16.key.pri $CERT_PATH/cert_16.cert 8105 > shard4_node16.log &

# Proxy (node 17)
nohup $SERVER_PATH $GLOBAL_CONFIG $CERT_PATH/node17.key.pri $CERT_PATH/cert_17.cert 8106 > proxy.log &

echo "All 16 shard nodes + proxy started"
