#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

/opt/flink/bin/start-cluster.sh

# Function to calculate the default date (one week ago)
get_default_date() {
    date -d "7 days ago" +"%Y%m%d"
}

# Check if a date is provided as a parameter/environment, otherwise use the default date
if [ -z "$1" ]; then
    DATE=$(get_default_date)
    echo "No date provided. Using default date: $DATE"
else
    DATE=$1
    echo "Using provided date: $DATE"
fi

#Download gdelt event for the date.
sh download_gdelt_event.sh $DATE

python3 ./gdelt_to_flink_iceberg.py $DATE

python3 ./provisioned-table.py

touch /tmp/ready

tail -f /dev/null