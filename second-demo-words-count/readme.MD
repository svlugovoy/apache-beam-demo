```
mvn clean package
mvn exec:java -Dexec.mainClass=com.svlugovoy.words.CountRunner -Dexec.args="--runner=DirectRunner”
mvn exec:java -Dexec.mainClass=com.svlugovoy.words.CountFilterRunner -Dexec.args="--runner=DirectRunner”
```