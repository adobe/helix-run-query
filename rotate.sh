#!/bin/bash

# check if DOMAINKEY parameter is set, exit if not
if [ -z "$DOMAINKEY" ]; then
  echo "DOMAINKEY environment variable is not set"
  exit 1
fi

DOMAINS=$@

# if there are no domains, read from stdin and store in DOMAINS
if [ -z "$DOMAINS" ]; then
  DOMAINS=$(cat)
fi

# if there are still no domains, exit
if [ -z "$DOMAINS" ]; then
  echo "No domains specified. Add them as command line arguments or pipe through standard input."
  exit 1
fi

echo "domain, key"
# loop over the domains
for DOMAIN in $DOMAINS; do
  KEY=$(curl -s -X "POST" "https://helix-pages.anywhere.run/helix-services/run-query@v3/rotate-domainkeys?url=$DOMAIN&graceperiod=-1&readonly=false" \
     -H "Authorization: Bearer $DOMAINKEY" | jq -r '.results.data[0].key')
  echo "$DOMAIN, $KEY"
done