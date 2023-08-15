#!/bin/bash

steering=$1
storage_node_ip_addr=$2

hostname=$(hostname)
log_file="${hostname}_program.log"

echo > $log_file  # clear

while true; do

  if [ -z "$steering" ]; then
    arg="-s 0"
  else
    arg="-s $steering"
  fi
 
  if [ -z "$storage_node_ip_addr" ]; then
    ip_arg=""
  else
    ip_arg="-a $storage_node_ip_addr"
  fi 
  
  python3 main.py $arg $ip_arg >> $log_file 2>&1

  # main.py가 종료되었을 때의 종료 코드를 확인합니다.
  # 만약 main.py가 sys.exit(1)로 종료되지 않았다면 스크립트를 종료합니다.
  # 그렇지 않으면 스크립트는 계속해서 다시 실행됩니다.
  exit_code=$?
  if [ "$exit_code" -ne 1 ]; then
    break
  fi
done
