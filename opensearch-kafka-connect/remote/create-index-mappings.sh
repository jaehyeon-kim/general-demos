#!/usr/bin/env bash
OPENSEARCH_ENDPOINT=$(terraform output -json | jq -r '.opensearch_domain_endpoint.value')
echo "OpenSearch endpoint - $OPENSEARCH_ENDPOINT ..."

echo "Create impressions index and field mapping"
curl -X PUT "https://$OPENSEARCH_ENDPOINT/impressions" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "bid_id": {
        "type": "text"
      },
      "created_at": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "campaign_id": {
        "type": "text"
      },
      "creative_details": {
        "type": "keyword"
      },
      "country_code": {
        "type": "keyword"
      }
    }
  }
}'

echo
echo "Create clicks index and field mapping"
curl -X PUT "https://$OPENSEARCH_ENDPOINT/clicks" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "correlation_id": {
        "type": "text"
      },
      "created_at": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "tracker": {
        "type": "text"
      }
    }
  }
}'

echo