#! /bin/bash

relative_path=`dirname $0`
root=`cd $relative_path;pwd`

dataset_path=$1
output_dir=$2
PLD=${*: 3}

jar="$root/target/abstat-statistics-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
cmd="java -cp  $jar  application.Statistics local[*]  $dataset_path $output_dir $PLD "

if eval $cmd; then
	cd $output_dir
	for dir in ./*
	do
		if [[ "$dir" == "./merged" ]]; then
	    	out="statistics.json"
	    else 
		    continue
		fi
		cat $dir/* >> $out            # union of files
	    rm -r $dir                    # remove spark output directory
	done
fi    
	
error=$?
if [[ $error != 0 ]]; then
	exit 1
fi