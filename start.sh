./gradlew build -x test
./gradlew -DuseFsVault="true" :launchers:connector_LF:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_VNB:shadowJar
./gradlew -DuseFsVault="true" :launchers:registrationservice:shadowJar
./docker_start.sh