#!/bin/bash

login="ftp-ugi03"
pass="tKS#g*xYCq"
host="ftp.mef.gob.pe"
local_dir="/tmp/datasets/"
log_file="/tmp/lftp-mirror.log"

lftp -u $login,$pass ftp://$host << EOF
ls -R > /tmp/lftp-mirror.log
quit
EOF
