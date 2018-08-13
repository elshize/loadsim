#!/bin/bash

show_log=0
random_seed=$((1 + RANDOM % 1000))
queue_type=5

while getopts ':cls:' option; do
  case "$option" in
    c)
        mvn install
        rm dependency-reduced-pom.xml
        ;;
    s)
        random_seed=$OPTARG
        ;;
    esac
done

for (( q=0; q<$queue_type; q++ ))
do  
    python3 paramGen.py $q $random_seed > "sample.param_$q" 
    java -jar target/loadsim-0.1.0.jar "sample.param_$q" 'sweep' > "temp_$q.log"  && rm "sample.param_$q" "temp_$q.log"
done

echo "Random seed $random_seed"
