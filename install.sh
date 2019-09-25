#!/bin/bash
# Find our package manager
chmod +x *.sh
if VERB="$( which apt-get )" 2> /dev/null; then
   echo "Debian-based"
   source install-ubuntu.sh
elif VERB="$( which yum )" 2> /dev/null; then
   echo "Red Hat-based"
  source install-rhel.sh
else
   echo "Installer script available for Debian- and Red Hat-based Linux systems only." >&2
   exit 1
fi
