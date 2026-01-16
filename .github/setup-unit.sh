if [ -z "${GOOGLE_SERVICE_ACCOUNT:-}" ]; then
  echo "GOOGLE_SERVICE_ACCOUNT not available"
  exit 0
fi

echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > ~/.gcp-service-account.json
echo "GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp-service-account.json" > $GITHUB_ENV