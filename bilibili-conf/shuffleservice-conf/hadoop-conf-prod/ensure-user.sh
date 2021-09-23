#!/usr/bin/env bash

function print_usage(){
  echo "Usage: [username]"
}

if [ $# -ne 1 ]; then
  print_usage
  exit 1
fi

username=$1
retry_time=3

while [ $retry_time -gt 0 ]
do
  if ! su - $username -c "whoami" >/dev/null 2>&1;then
    flock -x -w 10 /etc/passwd -c "sudo /usr/sbin/useradd $username"
  else
    exit 0
  fi
  retry_time=$(($retry_time - 1))
done