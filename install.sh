unzip -o build/libs/udb-java-0.0.2.jar -d build/libs/udb-java
rm -r -f ~/.udb/server/jar/udb-java/*
mv   build/libs/udb-java/* ~/.udb/server/jar/udb-java