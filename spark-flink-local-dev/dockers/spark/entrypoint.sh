#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# echo commands to the terminal output
set -ex

# Check whether there is a passwd entry for the container UID
myuid=$(id -u)
mygid=$(id -g)
# turn off -e for getent because it will return error code in anonymous uid case
set +e
uidentry=$(getent passwd $myuid)
set -e

# If there is no passwd entry for the container UID, attempt to create one
if [ -z "$uidentry" ] ; then
    if [ -w /etc/passwd ] ; then
        echo "$myuid:x:$myuid:$mygid:${SPARK_USER_NAME:-anonymous uid}:$SPARK_HOME:/bin/false" >> /etc/passwd
    else
        echo "Container ENTRYPOINT failed to add passwd entry for anonymous UID"
    fi
fi

if [ -n "${VALIDATION_SCRIPT_PATH}" ]; then
  python3 "${VALIDATION_SCRIPT_PATH}" 1>/dev/null 2> >(tee "${TERMINATION_ERROR_LOG_FILE_PATH}" >&2)
fi

SPARK_K8S_CMD="$1"

# Load spark-env explicity into executor. This is needed because executor
# does not run spark-submit and hence spark-env is not loaded into executor automatically
if [[ "$SPARK_K8S_CMD" == executor ]] ; then
    . "${SPARK_HOME}"/bin/load-spark-env.sh
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt
readarray -t SPARK_EXECUTOR_JAVA_OPTS < /tmp/java_opts.txt

if [ -n "$SPARK_EXTRA_CLASSPATH" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"
fi

if ! [ -z ${PYSPARK_PYTHON+x} ]; then
  export PYSPARK_PYTHON
fi
if ! [ -z ${PYSPARK_DRIVER_PYTHON+x} ]; then
  export PYSPARK_DRIVER_PYTHON
fi

# If HADOOP_HOME is set and SPARK_DIST_CLASSPATH is not set, set it here so Hadoop jars are available to the executor.
# It does not set SPARK_DIST_CLASSPATH if already set, to avoid overriding customizations of this value from elsewhere e.g. Docker/K8s.
if [ -n "${HADOOP_HOME}"  ] && [ -z "${SPARK_DIST_CLASSPATH}"  ]; then
  # Manually load Hadoop jars into Spark dependency
  export SPARK_DIST_CLASSPATH="$HADOOP_HOME/*:$HADOOP_HOME/lib/*"
fi

if ! [ -z ${HADOOP_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH";
fi

if ! [ -z ${SPARK_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH";
elif ! [ -z ${SPARK_HOME+x} ]; then
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH";
fi

# If dynamic sizing enabled then we can figure out the appropriate heap size
# by default the heap size used is always $SPARK_EXECUTOR_MEMORY
if [ -n "${DYNAMIC_SIZING_ENABLED}" ] && [ "$DYNAMIC_SIZING_ENABLED" = "true" ]; then
    MIN_OVERHEAD=$((384 * 1024 * 1024))
    if [ -z "${OVERHEAD_FACTOR}" ]; then
        OVERHEAD_FACTOR=0.1
        if [ -n "${DYNAMIC_SIZING_ENTRYPOINT}" ] && [[ "${DYNAMIC_SIZING_ENTRYPOINT}" = *.py ]]; then
            OVERHEAD_FACTOR=0.4
        fi
        if [ -n "${DYNAMIC_SIZING_ENTRYPOINT}" ] && [[ "${DYNAMIC_SIZING_ENTRYPOINT}" = *.R ]]; then
            OVERHEAD_FACTOR=0.4
        fi
        if [ -n "${DYNAMIC_SIZING_ENTRYPOINT}" ] && [[ "${DYNAMIC_SIZING_ENTRYPOINT}" = *.r ]]; then
            OVERHEAD_FACTOR=0.4
        fi
    fi
    if [ -z "${PYSPARK_MEM}" ]; then
        PYSPARK_MEM=0
    fi
    OVERHEAD_WITHOUT_MIN=$(bc <<< "($EXEC_POD_MEM_REQUEST - $PYSPARK_MEM) * $OVERHEAD_FACTOR/(1+$OVERHEAD_FACTOR)")
    OVERHEAD=$(( OVERHEAD_WITHOUT_MIN > MIN_OVERHEAD ? OVERHEAD_WITHOUT_MIN : MIN_OVERHEAD ))
    SPARK_EXECUTOR_MEMORY=$(bc <<< "$EXEC_POD_MEM_REQUEST - $OVERHEAD - $PYSPARK_MEM")
fi

case "$SPARK_K8S_CMD" in
  driver)
    shift 1
    ${SPARK_HOME}/sbin/start-master.sh
    while true; do
      sleep 10000
    done
    # CMD=(
    #   "$SPARK_HOME/bin/spark-submit"
    #   --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
    #   --deploy-mode client
    #   "$@"
    # )
    ;;
  executor)
    shift 1
    ${SPARK_HOME}/sbin/start-worker.sh ${SPARK_DRIVER_URL} \
      --cores $SPARK_EXECUTOR_CORES --memory $SPARK_EXECUTOR_MEMORY    
    while true; do
      sleep 10000
    done
    # CMD=(
    #   ${JAVA_HOME}/bin/java
    #   "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
    #   -Xms$SPARK_EXECUTOR_MEMORY
    #   -Xmx$SPARK_EXECUTOR_MEMORY
    #   -cp "$SPARK_CLASSPATH:$SPARK_DIST_CLASSPATH"
    #   org.apache.spark.executor.CoarseGrainedExecutorBackend
    #   --driver-url $SPARK_DRIVER_URL
    #   --executor-id $SPARK_EXECUTOR_ID
    #   --cores $SPARK_EXECUTOR_CORES
    #   --app-id $SPARK_APPLICATION_ID
    #   --hostname $SPARK_EXECUTOR_POD_IP
    #   --resourceProfileId $SPARK_RESOURCE_PROFILE_ID
    # )
    ;;
  job)
    shift 1
    CMD=(
      "$SPARK_HOME/bin/spark-submit"
      "$@"
    )
    ;;
  *)
    echo "Non-spark-on-k8s command provided, proceeding in pass-through mode..."
    CMD=("$@")
    ;;
esac

# # Initialize and Check if Log Rotation is enabled from CP
# # When set to True, we will write logs using tee else we use multilog
# DISABLE_CONTAINER_LOG_ROTATION=1
# if [ -z "$CONTAINER_LOG_ROTATION" ]; then
#     DISABLE_CONTAINER_LOG_ROTATION=0
# fi
# if [ -z "$MULTILOG_SIZE_KEY" ]; then
#     export MULTILOG_SIZE_KEY=s134217728
# fi
# if [ -z "$MULTILOG_NUM_KEY" ]; then
#     export MULTILOG_NUM_KEY=3
# fi

# # Execute the container CMD under tini for better hygiene

# DISABLE_STDOUT_STDERR=0
# if [ -z "$K8S_SPARK_LOG_URL_STDOUT" ] || [ -z "$K8S_SPARK_LOG_URL_STDERR" ]; then
#     DISABLE_STDOUT_STDERR=1
# fi

# DISABLE_PULLING_CONTAINER_FAILURE=0
# if [ -z "$K8S_SPARK_LOG_ERROR_REGEX" ] || [ -z "$TERMINATION_ERROR_LOG_FILE_PATH" ]; then
#   DISABLE_PULLING_CONTAINER_FAILURE=1
# fi

# if [ -n "$POD_METADATA_PATH" ]; then
#   mkdir -p "$(dirname "$POD_METADATA_PATH" )"
# fi

# if [ -n "$CONTAINER_RESOURCE_CPU_FILE_PATH" ] && [ -n "$SPARK_MAIN_CONTAINER_CPU_REQUEST" ]; then
#     mkdir -p "$(dirname "$CONTAINER_RESOURCE_CPU_FILE_PATH")"
#     echo "$SPARK_MAIN_CONTAINER_CPU_REQUEST" > "$CONTAINER_RESOURCE_CPU_FILE_PATH"
# fi

# if [ -n "$CONTAINER_RESOURCE_MEMORY_FILE_PATH" ] && [ -n "$SPARK_MAIN_CONTAINER_MEMORY_REQUEST" ]; then
#     mkdir -p "$(dirname "$CONTAINER_RESOURCE_MEMORY_FILE_PATH")"
#     echo "$SPARK_MAIN_CONTAINER_MEMORY_REQUEST" > "$CONTAINER_RESOURCE_MEMORY_FILE_PATH"
# fi

# mkdir -p "$(dirname "$K8S_SPARK_LOG_URL_STDERR" )" "$(dirname "$K8S_SPARK_LOG_URL_STDOUT" )"

# if [ -n "${SIDECAR_SIGNAL_FILE}"  ]; then # Check for side car
#   args=(-f "$SIDECAR_SIGNAL_FILE")
#   if [ -n "$SIDECAR_ERROR_FOLDER_PATH" ]; then args+=(-e "$SIDECAR_ERROR_FOLDER_PATH"); fi

#   if (($DISABLE_STDOUT_STDERR)); then # Check if Logging stdout and stderr is disabled

#     if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#       /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}"
#     else
#       /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#     fi
#   else


#     if (($DISABLE_CONTAINER_LOG_ROTATION)); then
#         if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#             /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee $K8S_SPARK_LOG_URL_STDOUT) 2> >(tee $K8S_SPARK_LOG_URL_STDERR >&2)
#         else
#             /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee $K8S_SPARK_LOG_URL_STDOUT >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee $K8S_SPARK_LOG_URL_STDERR >&2  >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#         fi
#     else
#         if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#             /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDOUT)) 2> >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDERR) >&2)
#         else
#             /usr/bin/pub-terminate.sh "${args[@]}" & exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDOUT) >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDERR) >&2  >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#         fi
#     fi
#   fi
# else
#   if (($DISABLE_STDOUT_STDERR)); then # Check if Logging stdout and stderr is disabled
#     if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#       exec /usr/bin/tini -s -- "${CMD[@]}"
#     else
#       exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#     fi

#   else
#         if (($DISABLE_CONTAINER_LOG_ROTATION)); then
#             if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#               exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee $K8S_SPARK_LOG_URL_STDOUT) 2> >(tee $K8S_SPARK_LOG_URL_STDERR >&2)
#             else
#               exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee $K8S_SPARK_LOG_URL_STDOUT >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee $K8S_SPARK_LOG_URL_STDERR >&2 >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#             fi
#         else
#             if ((DISABLE_PULLING_CONTAINER_FAILURE)); then # Check if pull container failure is disabled
#                 exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDOUT)) 2> >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDERR) >&2)
#             else
#                 exec /usr/bin/tini -s -- "${CMD[@]}" > >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDOUT) >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH)) 2> >(tee >(multilog $MULTILOG_SIZE_KEY $MULTILOG_NUM_KEY $K8S_SPARK_LOG_URL_STDERR) >&2 >(stdbuf -o0 grep -E "$K8S_SPARK_LOG_ERROR_REGEX" >> $TERMINATION_ERROR_LOG_FILE_PATH))
#             fi
#         fi
#     fi
# fi