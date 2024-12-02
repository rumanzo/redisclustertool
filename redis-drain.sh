#!/bin/bash

# redis-drain.sh: failover all redis masters on 1 host, save them into file

while :
do
    case "$1" in
        -h | --host)
            host=$2
            shift 2
            ;;
        -p | --port)
            port=$2
            shift 2
            ;;
        -s | --sleep)
            sleep_time=$2
            shift 2
            ;;
        -*) # unknown option
            echo "Error: Unknown option: $1" >&2
            exit 1
            ;;
        *) # no more options
            break
            ;;
    esac
done

usage='
redis-drain.sh: drain all redis masters from 1 host, save them into a file

Usage:
    redis-drain.sh -h <host-where-to-failover-masters.example.com> -p <port> [-s <sleep_time>]

Example:
    redis-drain.sh -h <host> -p <port> -s 45
'
if [[ -z "$port" ]];
then
    echo "$usage";
    exit 0;
fi

set -o errexit
set -o pipefail

# Create a temporary file to store the output
tmpfile=$(mktemp)
echo "Saving ids to $tmpfile"

redis-cli -h $host -p $port -c CLUSTER NODES | grep $host | grep master | awk '{print $1}' > "$tmpfile"
slaves=$(cat "$tmpfile" | while read line; do redis-cli -c -h $host -p $port CLUSTER NODES | grep $line | grep slave | awk '{print $2}' | cut -d : -f 1,2 --output-delimiter ' -p ' | head -n 1; done)

set +o pipefail
set +o errexit 

echo "$slaves" | while read line ; do

	cur_host=$(echo "$line" | cut -d' ' -f1)
	cur_port=$(echo "$line" | cut -d' ' -f3)
	host_and_port="$cur_host:$cur_port"
    is_master=$(redis-cli -h $host -p $port -c CLUSTER NODES | grep $host_and_port | grep master)
    if [[ ! -z "$is_master" ]]; then
		# already master 
		continue
	fi
	
    response=$(redis-cli -h $cur_host -p $cur_port -c CLUSTER FAILOVER)
    sleep $(($sleep_time/2))
    is_master2=$(redis-cli -h $host -p $port -c CLUSTER NODES | grep $host_and_port | grep master)

    if [[ ! -z "$is_master2" ]]; then
        echo "Failover successful for $host_and_port"
    else
        echo "Failover failed for $host_and_port"
        echo "Response: $response $is_master"
    fi
    sleep $(($sleep_time/2))
done

echo "Checking if all masters have been failovered"
not_failovered_masters=$(redis-cli -h $host -p $port -c CLUSTER NODES | grep $host | grep master) 

if [[ -z "$not_failovered_masters" ]]; then
    echo "All masters have been failovered"
else
    echo "Error: Failed to failover the following masters: $not_failovered_masters"
    exit 1
fi
