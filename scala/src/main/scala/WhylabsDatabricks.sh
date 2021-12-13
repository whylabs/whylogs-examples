#!/bin/sh
PROPERTIES_FILE="/databricks/spark/dbconf/java/extra.security"
# by default the disabled algorithms include this set and GCM, which has known performance issues on parquet files
DISABLED_ALGOS="SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL"
echo "Configure Databricks java for Whylabs access and allow GCM"
if [[ -f "${PROPERTIES_FILE}" ]]; then
    echo "setting jdk.tls.disabledAlgorithms..."
    echo "jdk.tls.disabledAlgorithms=${DISABLED_ALGOS}" | tee "${PROPERTIES_FILE}"
else
    >&2 echo "ERROR failed to find ${PROPERTIES_FILE}"
fi
