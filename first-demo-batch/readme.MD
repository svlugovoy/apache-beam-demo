```
mvn clean package
mvn exec:java -Dexec.mainClass=com.svlugovoy.batch.SalesPerCarsBrand -Pdirect-runner -Dexec.args="--runner=DirectRunner”
```