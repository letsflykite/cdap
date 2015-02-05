#!/bin/bash

#
# Copyright © 2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

export TWILL_FS_USER="cdap"
export TWILL_APP_DIR="hdfs://10.240.249.204/cdap/twill/master.services/a981714f-c3d7-46a2-842a-065e8bdeec1b"
export JAVA_HOME="/usr/lib/jvm/java"
export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
"
export HADOOP_YARN_HOME="/usr/lib/hadoop-yarn"
export NM_HOST="secure-hive-3node950-1001.dev.continuuity.net"
export YARN_CONTAINER_ID="container_1423101766359_0002_01_000010"
export JVM_PID="$$"
export YARN_CONTAINER_MEMORY_MB="1024"
export TWILL_INSTANCE_ID="0"
export YARN_CONTAINER_HOST="secure-hive-3node950-1001.dev.continuuity.net"
export PWD="/data/yarn/local/usercache/cdap/appcache/application_1423101766359_0002/container_1423101766359_0002_01_000010"
export NM_PORT="43998"
export LOGNAME="cdap"
export TWILL_APP_NAME="master.services"
export MALLOC_ARENA_MAX="4"
export YARN_CONTAINER_VIRTUAL_CORES="1"
export LOG_DIRS="/data/log/hadoop-yarn/userlogs/application_1423101766359_0002/container_1423101766359_0002_01_000010"
export NM_HTTP_PORT="8042"
export LOCAL_DIRS="/data/yarn/local/usercache/cdap/appcache/application_1423101766359_0002"
export TWILL_RUN_ID="444f0a2a-4bb1-4619-acb5-db1ed3b1f5d7-0"
export HADOOP_COMMON_HOME="/usr/lib/hadoop"
export TWILL_INSTANCE_COUNT="1"
export TWILL_RUNNABLE_NAME="explore.service"
export TWILL_LOG_KAFKA_ZK="secure-hive-3node950-1000.dev.continuuity.net:2181/cdap/twill/master.services/a981714f-c3d7-46a2-842a-065e8bdeec1b/kafka"
export HADOOP_TOKEN_FILE_LOCATION="/data/yarn/local/usercache/cdap/appcache/application_1423101766359_0002/container_1423101766359_0002_01_000010/container_tokens"
export USER="cdap"
export HADOOP_HDFS_HOME="/usr/lib/hadoop-hdfs"
export TWILL_ZK_CONNECT="secure-hive-3node950-1000.dev.continuuity.net:2181/cdap/twill/master.services"
export CONTAINER_ID="container_1423101766359_0002_01_000010"
export HOME="/home/"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export YARN_CONTAINER_PORT="43998"
export TWILL_APP_RUN_ID="a981714f-c3d7-46a2-842a-065e8bdeec1b"
exec /bin/bash -c "$JAVA_HOME/bin/java -Djava.io.tmpdir=tmp -Dyarn.container=$YARN_CONTAINER_ID -Dtwill.runnable=$TWILL_APP_NAME.$TWILL_RUNNABLE_NAME -cp launcher.jar:$HADOOP_CONF_DIR -Xmx774m -verbose:gc -Xloggc:/data/log/hadoop-yarn/userlogs/application_1423101766359_0002/container_1423101766359_0002_01_000010/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M org.apache.twill.launcher.TwillLauncher container.jar org.apache.twill.internal.container.TwillContainerMain true 1>/data/log/hadoop-yarn/userlogs/application_1423101766359_0002/container_1423101766359_0002_01_000010/stdout 2>/data/log/hadoop-yarn/userlogs/application_1423101766359_0002/container_1423101766359_0002_01_000010/stderr"
