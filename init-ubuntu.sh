#!/bin/sh
### BEGIN INIT INFO
# Provides:          pasir-classr
# Required-Start:    $local_fs $remote_fs $network
# Required-Stop:     $local_fs $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: PASIR/Classr API
# Description:       JSON/RPC API over HTTP for ticket classification for IBM PASIR
### END INIT INFO


# Documentation available at
# http://refspecs.linuxfoundation.org/LSB_3.1.0/LSB-Core-generic/LSB-Core-generic/iniscrptfunc.html
# Debian provides some extra functions though
. /lib/lsb/init-functions


DAEMON_NAME="pasir-classr"
DAEMON_USER="classrapi"
DAEMON_PATH="/usr/bin/python"
DAEMON_OPTS="server.py"
DAEMON_PWD="/opt/pasir-classr"
DAEMON_DESC=$(get_lsb_header_val $0 "Short-Description")
DAEMON_PID="/var/run/${DAEMON_NAME}.pid"
DAEMON_NICE=0

[ -r "/etc/default/${DAEMON_NAME}" ] && . "/etc/default/${DAEMON_NAME}"

do_start() {
  local result

	pidofproc -p "${DAEMON_PID}" "${DAEMON_PATH}" > /dev/null
	if [ $? -eq 0 ]; then
		log_warning_msg "${DAEMON_NAME} is already started"
		result=0
	else
		log_daemon_msg "Starting ${DAEMON_DESC}" "${DAEMON_NAME}"
		if [ -z "${DAEMON_USER}" ]; then
			start-stop-daemon --start --quiet --oknodo --background \
				--nicelevel $DAEMON_NICE \
				--chdir "${DAEMON_PWD}" \
				--pidfile "${DAEMON_PID}" --make-pidfile \
				--exec "${DAEMON_PATH}" $DAEMON_OPTS
			result=$?
		else
			start-stop-daemon --start --quiet --oknodo --background \
				--nicelevel $DAEMON_NICE \
				--chdir "${DAEMON_PWD}" \
				--pidfile "${DAEMON_PID}" --make-pidfile \
				--chuid "${DAEMON_USER}" \
				--exec "${DAEMON_PATH}" $DAEMON_OPTS
			result=$?
		fi
		log_end_msg $result
	fi
	return $result
}

do_stop() {
	local result

	pidofproc -p "${DAEMON_PID}" "${DAEMON_PATH}" > /dev/null
	if [ $? -ne 0 ]; then
		log_warning_msg "${DAEMON_NAME} is not started"
		result=0
	else
		log_daemon_msg "Stopping ${DAEMON_DESC}" "${DAEMON_NAME}"
		start-stop-daemon --stop --signal 9 --pidfile "${DAEMON_PID}"
		result=$?
		log_end_msg $result
		rm "${DAEMON_PID}"
	fi
	return $result
}

do_restart() {
	local result
	do_stop
	result=$?
	if [ $result = 0 ]; then
		do_start
		result=$?
	fi
	return $result
}

do_status() {
	local result
	status_of_proc -p "${DAEMON_PID}" "${DAEMON_PATH}" "${DAEMON_NAME}"
	result=$?
	return $result
}

do_usage() {
	echo $"Usage: $0 {start | stop | restart | status}"
	exit 1
}

case "$1" in
start)   do_start;   exit $? ;;
stop)    do_stop;    exit $? ;;
restart) do_restart; exit $? ;;
status)  do_status;  exit $? ;;
*)       do_usage;   exit  1 ;;
esac