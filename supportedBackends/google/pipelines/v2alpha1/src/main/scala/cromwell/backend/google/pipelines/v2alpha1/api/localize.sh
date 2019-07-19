#!/usr/bin/env bash


rp_flag="-u someproj"
copy_flag=""
first_file=true

set -- foo bar baz qux

while [[ $# -gt 0 ]]; do
  i=0
  while [[ ${i} -lt 3 ]]; do
    cloud="$1"
    host="$2"
    copyit "$copy_flag" "$1" "$2" > copyit.log
    if [[ $? = 0 ]]; then
      shift; shift
      break
    else
      if [[ first_file = true && "$copy_flag" = "" ]]; then
        # if this is the first time through and it's the first file then look for rp errors otherwise don't bother
        if grep -q "you must pay" copyit.log; then
          copy_flag="$rp_flag"
          first_file=false # paranoia in case it looks like an rp failure but setting the rp flag doesn't help
        fi
      else
        i=$((i+1))
      fi
    fi
  done
  first_file=false
done
