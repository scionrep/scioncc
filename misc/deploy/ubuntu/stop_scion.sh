#!/bin/bash
psid=$(ps aux | grep '[r]es/deploy/basic.yml' | awk '{print $2}')
kill $psid
rm "/home/ubuntu/dev/scioncc/manhole-$psid.json"
