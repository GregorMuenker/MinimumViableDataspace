#!/bin/bash

set -euxo pipefail

# Split env vars into an array using delimiter ':'

ParticipantIdArray=(${PARTICIPANT_ID//:/ })
AssetsStorageAccountArray=(${ASSETS_STORAGE_ACCOUNT//:/ })
AssetsStorageKeyArray=(${ASSETS_STORAGE_KEYS//:/ })
EdcHostArray=(${EDC_HOST//:/ })

if [ ${#ParticipantIdArray[@]} -ne ${#AssetsStorageAccountArray[@]} ] || [ ${#AssetsStorageAccountArray[@]} -ne ${#EdcHostArray[@]} ]; then
  echo "PARTICIPANT_ID,ASSETS_STORAGE_ACCOUNT and EDC_HOST must be of equal length"
  exit 1
fi

curl -sL https://aka.ms/InstallAzureCLIDeb | bash

for i in "${!EdcHostArray[@]}"; do
  echo "Seeding data for Participant ID: ${ParticipantIdArray[$i]}, Assets Storage Account: ${AssetsStorageAccountArray[$i]}, EDC Host: ${EdcHostArray[$i]}"

  conn_str="DefaultEndpointsProtocol=http;AccountName=${AssetsStorageAccountArray[$i]};AccountKey=${AssetsStorageKeyArray[$i]};BlobEndpoint=http://azurite:10000/${AssetsStorageAccountArray[$i]};"
  az storage container create --name src-container --connection-string $conn_str
  az storage blob upload -f /deployment/azure/terraform/modules/participant/sample-data/text-document.txt --container-name src-container --name text-document.txt --connection-string $conn_str

  newman run \
    --folder "Publish Master Data" \
    --env-var data_management_url="http://${EdcHostArray[$i]}:9191/api/v1/data" \
    --env-var storage_account="${AssetsStorageAccountArray[$i]}" \
    --env-var participant_id="${ParticipantIdArray[$i]}" \
    --env-var api_key="$API_KEY" \
    deployment/data/MVD.postman_collection_${ParticipantIdArray[$i]}.json
done
