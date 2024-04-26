cluster_account=$1

EKS_ACCOUNT_PRIVATE_CREDENTIALS_FILE=/var/lib/airflow/.eks-account.private.credentials

if ! [[ -f $EKS_ACCOUNT_PRIVATE_CREDENTIALS_FILE ]] ; then
    echo "$EKS_ACCOUNT_PRIVATE_CREDENTIALS_FILE could not be found, exiting..."
    exit 2
fi

unset TO_USE_CREDENTIALS_FILE
case $cluster_account in
    "private")
      TO_USE_CREDENTIALS_FILE=$EKS_ACCOUNT_PRIVATE_CREDENTIALS_FILE
      cp /var/lib/airflow/.kube/privateconfig /var/lib/airflow/.kube/config
      ;;
    *)
      echo "Attempting to connect to unrecognized cluster $cluster_account, exiting..."
      exit 2
      ;;
esac

SERVICE_ACCOUNT_NUMBER="$(grep -rh eks.account.number $TO_USE_CREDENTIALS_FILE | sed 's/eks.account.number=//g')"
SERVICE_ACCOUNT_ROLE="$(grep -rh eks.role $TO_USE_CREDENTIALS_FILE | sed 's/eks.role=//g')"
SERVICE_ACCOUNT_NAME="$(grep -rh eks.account.name $TO_USE_CREDENTIALS_FILE | sed 's/eks.account.name=//g')"
SERVICE_ACCOUNT_PASSWORD="$(grep -rh eks.password $TO_USE_CREDENTIALS_FILE | sed 's/eks.password=//g')"

if [ -z "$SERVICE_ACCOUNT_NUMBER" ] | [ -z "$SERVICE_ACCOUNT_NAME" ] | [ -z "$SERVICE_ACCOUNT_PASSWORD" ] | [ -z "$SERVICE_ACCOUNT_ROLE" ] ; then
    echo "Missing required K8S EKS environment variables , exiting..."
    exit 2
fi

set -euo pipefail

saml2aws --role arn:aws:iam::$SERVICE_ACCOUNT_NUMBER:role/$SERVICE_ACCOUNT_ROLE login --force --mfa=Auto --username=$SERVICE_ACCOUNT_NAME --password=$SERVICE_ACCOUNT_PASSWORD --skip-prompt

set +uo pipefail