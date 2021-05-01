ZEPHOME=/Users/jac/tech/zeppelin-0.9.0-bin-netinst/
(cd $PWD/../executor/spark && exec sbt package)
(cd $ZEPHOME/bin && sh zeppelin-daemon.sh restart)
echo "DONE!!"
