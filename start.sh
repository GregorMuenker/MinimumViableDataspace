./gradlew build -x test
./gradlew -DuseFsVault="true" :launchers:connector_LF_1:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_LF_2:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_VNB:shadowJar
./gradlew -DuseFsVault="true" :launchers:registrationservice:shadowJar
./docker_start.sh