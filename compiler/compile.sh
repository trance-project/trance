
JSON=../executor/spark/src/main/scala/sparkutils/generated/GenerateCosts.json
#NOTE=2G1GEVEUX
NOTE=2G4DWEWA6
ZEP=qc-3:8085

if [ $1="notebk" ]; then
 (curl --request POST --header "Content-Type: application/json" --data @$JSON http://$ZEP/api/notebook/$NOTE/paragraph > tmp)
 PARA=$(cat tmp | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["body"])')
 (curl --request POST --header "Content-Type: application/json" http://$ZEP/api/notebook/run/$NOTE/$PARA > tmp)
 (cat tmp | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["body"]["msg"][0]["data"])' > out)
 (curl --request DELETE --header "Content-Type: application/json" http://$ZEP/api/notebook/$NOTE/paragraph/$PARA)
else
  (cd $PWD/../executor/spark && exec sbt package)
  (sh $PWD/../executor/spark/run.sh GenerateCosts > out)
fi
