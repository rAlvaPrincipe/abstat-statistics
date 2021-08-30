#!/bin/bash

function start_enviroment(){
	echo "Starting MongoDB"
	docker start my_mongo
}


function stop_enviroment(){
	docker stop my_mongo
}


function start(){
	echo "Starting manager"
	cd manager
	java -Xmx4000m -Xms256m -jar target/prova-0.0.1-SNAPSHOT.jar
}


function build(){
	cd manager
	mvn package --quiet -Dmaven.test.skip=true -Dstart-class=application.Application

	cd ..
	cd abstat-statistics
	mvn package --quiet -Dmaven.test.skip=true -Dstart-class=application.Statistics

}

case "$1" in
        start)
			start 
            	;;
        startenv)
			start_enviroment 
            	;;
        stopenv)
			stop_enviroment 
        	;;
	build)
			build
		;;
        *)
        		echo "Usage: start | startenv | stopenv | build "
		;;
esac