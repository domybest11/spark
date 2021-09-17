#!/usr/bin/env bash

function print_usage(){
  echo "Usage: [username]"
}

if [ $# -ne 1 ]; then
  print_usage
  exit 1
fi

username=$1
retry_time=10

while [ $retry_time -gt 0 ]
do
  if sudo su - $username -c "whoami" >/dev/null 2>&1;then
    exit 0
  fi
  retry_time=$(($retry_time - 1))
done
echo "get user from AD failed!"
exit 1