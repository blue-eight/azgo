export AZURE_SUBSCRIPTION=$(az account show | jq -r '.id')
export RESOURCE_GROUP='210900-aks'
export AKS_NAME='aks1'
