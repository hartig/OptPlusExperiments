#!/bin/bash

for f in measurements-*.csv
do
	echo "$f" >> measurements.csv
	cat $f >> measurements.csv
done
