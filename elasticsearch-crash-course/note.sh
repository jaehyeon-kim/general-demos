###### PART 1
# https://www.youtube.com/playlist?list=PL_mJOmq4zsHZYAyK606y7wjQtC0aoE6Es
# https://github.com/LisaHJung/Beginners-Crash-Course-to-Elastic-Stack-Series-Table-of-Contents

curl localhost:9200

# jq, python -m json.tool
curl localhost:9200/_cluster/health | json_pp
curl localhost:9200/_nodes/stats | json_pp

#### CRUD
curl -X PUT localhost:9200/favourite_candy
curl localhost:9200/ | json_pp

## id will be auto-generated
curl -X POST localhost:9200/favourite_candy/_doc \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"Lisa","candy":"Sour Skittles"}'
curl localhost:9200/favourite_candy/_doc/qoix2IkBueezf-_gAixr | json_pp

## upsert a document, version will be updated for update
curl -X PUT localhost:9200/favourite_candy/_doc/0 \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"John","candy":"Starburst"}'
curl localhost:9200/favourite_candy/_doc/0 | json_pp

## fail if id exists
curl -X PUT localhost:9200/favourite_candy/_create/0 \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"John","candy":"Starburst"}'

## update
curl -X POST localhost:9200/favourite_candy/_update/0 \
  -H 'Content-Type: application/json' \
  -d '{"doc":{"candy": "Lolli Pop"}}'

## delete
curl -X DELETE localhost:9200/favourite_candy/_doc/0

###### PART 2

# Precision = True positives / (True positives <relevant and retrieved> + False positives <irrelevant but retrieved>)
#  => what portition of the retrieved data is actually relevant to the search query 
# Recall = True positives / (True positives + False negatives <relevant but not retrieved>)
#  => what portiton of relevant data is being returned as search results?

# Precision focuses on relevance (quality) while recall on quantity, they are inversely related

# search results are ranked by score

# score? 
#   term frequency (TF) - how many times each search term appears in a document
#   inverse document frequency (IDF) - if some terms appear in many documents, they are consiered as unimportant

# dataset - https://www.kaggle.com/datasets/rmisra/news-category-dataset

```
GET news_headlines/_search

GET news_headlines/_search
{
  "track_total_hits": true
}

GET news_headlines/_search
{
  "query": {
    "range": {
      "date": {
        "gte": "2015-06-20",
        "lte": "2015-09-22"
      }
    }
  }
}

GET news_headlines/_search
{
  "aggs": {
    "by_category": {
      "terms": {
        "field": "category",
        "size": 100
      }
    }
  }
}

GET news_headlines/_search
{
  "query": {
    "match": {
      "category": "ENTERTAINMENT"
    }
  },
  "aggs": {
    "popular_in_entertainment": {
      "significant_text": {
        "field": "headline"
      }
    }
  }
}

GET news_headlines/_search
{
  "query": {
    "match": {
      "headline": {
        "query": "Khloe Kardashian Kendall Jenner"
      }
    }
  }
}

GET news_headlines/_search
{
  "query": {
    "match": {
      "headline": {
        "query": "Khloe Kardashian Kendall Jenner",
        "operator": "and"
      }
    }
  }
}

GET news_headlines/_search
{
  "query": {
    "match": {
      "headline": {
        "query": "Khloe Kardashian Kendall Jenner",
        "minimum_should_match": 3
      }
    }
  }
}

DELETE news_headlines

GET news_headlines/_search
```
