./gradlew -DuseFsVault="true" :launchers:connector_LF_1:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_LF_2:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_VNB:shadowJar
./gradlew -DuseFsVault="true" :launchers:registrationservice:shadowJar
docker-compose -f system-tests/docker-compose.yml up -d --build