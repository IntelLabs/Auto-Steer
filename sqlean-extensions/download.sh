#!/bin/bash

ARCH=x86

echo "Download sqlean extensions for Linux arch=${ARCH}"
rm -f sqlean-linux-${ARCH}.zip
wget https://github.com/nalgeon/sqlean/releases/latest/download/sqlean-linux-${ARCH}.zip
unzip -p sqlean-linux-${ARCH}.zip stats.so > stats.so
rm -f sqlean-linux-${ARCH}.zip

