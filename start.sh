./gradlew -DuseFsVault="true" :launchers:connector_LF:shadowJar
./gradlew -DuseFsVault="true" :launchers:connector_VNB:shadowJar
./gradlew -DuseFsVault="true" :launchers:registrationservice:shadowJar
docker-compose -f system-tests/docker-compose.yml up -d --build