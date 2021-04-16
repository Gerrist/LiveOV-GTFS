#!/bin/bash
rm -rf build
tsc
cd build
mkdir out
mkdir tmp
node --max_old_space_size=8000 downloadCHB.js
node --max_old_space_size=8000 handleCHB.js
json2csv -i out/stop-areas.json -o tmp/stop-areas.csv
json2csv -i out/stops.json -o tmp/chb-stops.csv
rm -rf out
wget http://gtfs.ovapi.nl/nl/gtfs-nl.zip
unzip gtfs-nl.zip -d tmp
rm gtfs-nl.zip
node gtfs
