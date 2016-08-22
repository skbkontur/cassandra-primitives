@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

@REM automatic JAVA_HOME discover, thanks to http://chriskopec.com/blog/2009/jun/15/automatic-discovery-java_home-batch-file/

FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment" /v CurrentVersion') DO set CurVer=%%B
FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment\%CurVer%" /v JavaHome') DO set JAVA_HOME=%%B

setlocal
call "%~dp0zkEnv.cmd"

set JAVA_OPTS=-ea^
 -Xms8G^
 -Xmx8G^
 -XX:+HeapDumpOnOutOfMemoryError^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:SurvivorRatio=8^
 -XX:MaxTenuringThreshold=1^
 -XX:CMSInitiatingOccupancyFraction=75^
 -XX:+UseCMSInitiatingOccupancyOnly^
 -Dcom.sun.management.jmxremote.port={{jmxPort}}^
 -Dcom.sun.management.jmxremote.ssl=false^
 -Dcom.sun.management.jmxremote.authenticate=false^
 -XX:+UnlockCommercialFeatures^
 -XX:+FlightRecorder

set ZOOMAIN=org.apache.zookeeper.server.quorum.QuorumPeerMain
echo on
call %JAVA% "-Dzookeeper.log.dir=%ZOO_LOG_DIR%" "-Dzookeeper.root.logger=%ZOO_LOG4J_PROP%" %JAVA_OPTS% -cp "%CLASSPATH%" %ZOOMAIN% "%ZOOCFG%" %*

endlocal
