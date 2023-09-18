#!/usr/bin/env bash
echo "Create impressions index and field mapping"
curl -X PUT "https://localhost:9200/impressions" -ku admin:admin -H 'Content-Type: application/json' -d'
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
curl -X PUT "https://localhost:9200/clicks" -ku admin:admin -H 'Content-Type: application/json' -d'
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