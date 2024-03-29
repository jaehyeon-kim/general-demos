## [Part 1: Intro to Elasticsearch & Kibana](https://github.com/LisaHJung/Part-1-Intro-to-Elasticsearch-and-Kibana)

```bash
## info about cluster and nodes
GET _cluster/health
GET _nodes/stats

## CREATE
# create an index
PUT favorite_candy
GET _cat/indices?format=json

# index a document with POST - id autogenerated
POST favorite_candy/_doc
{
  "first_name": "jaehyeon",
  "candy": "Sour Skittles"
}
GET favorite_candy/_doc/1anN_IkBRlQ7K36qPxJL

# index a document with PUT - specific id
PUT favorite_candy/_doc/1
{
  "first_name": "bernie",
  "candy": "Starburst"
}
GET favorite_candy/_doc/1

# PUT is basically upsert
PUT favorite_candy/_doc/1
{
  "first_name": "bernie kim",
  "candy": "Starburst"
}

# _create endpoint is strictly create
PUT favorite_candy/_create/1 # 409 error
{
  "first_name": "bernie again",
  "candy": "Starburst"
}

PUT favorite_candy/_create/2
{
  "first_name": "john",
  "candy": "Lolli Pop"
}

## READ
GET favorite_candy/_doc/1

GET favorite_candy/_search
{
  "query": {
    "match_all": {}
  }
}

## UPDATE
POST favorite_candy/_update/1
{
  "doc": {
    "candy": "M&M's"
  }
}

## DELETE
DELETE favorite_candy/_doc/1
```

## [Part 2: Understanding the relevance of your search with Elasticsearch and Kibana](https://github.com/LisaHJung/Part-2-Understanding-the-relevance-of-your-search-with-Elasticsearch-and-Kibana-)

```bash
## queries
GET news_headlines/_search

# get exact total number of hits
GET news_headlines/_search
{
  "track_total_hits": true
}

# search for data within a specific time range
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

## aggregations
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

## combination of query and aggregation
# search for the most significant term in a category
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

## precision and recall
# increase recall
#   - by default, match query uses 'OR' operator (retrieves if any matches), order and proximity not taken into account
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

# increase precision with 'AND' operator (retrieves only if all match)
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

# balance precision and recall
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
```

## [Part 3: Running full text queries and combined queries with Elasticsearch and Kibana](https://github.com/LisaHJung/Part-3-Running-full-text-queries-and-combined-queries-with-Elasticsearch-and-Kibana)

```bash
## full text queries
# search for search terms
#   - basically we can use match query
#   - can be misleading when searching for phrases
GET news_headlines/_search
{
  "query": {
    "match": {
      "headline": {
        "query": "Shape of you"
      }
    }
  }
}

# search for phrases
GET news_headlines/_search
{
  "query": {
    "match_phrase": {
      "headline": "Shape of You"
    }
  }
}

# running a match query against multiple fields
GET news_headlines/_search
{
  "query": {
    "multi_match": {
      "query": "Michelle Obama",
      "fields": [
        "headline",
        "short_description",
        "authors"
      ]
    }
  }
}

# pre-field boosting to improve precision with the same recall
GET news_headlines/_search
{
  "query": {
    "multi_match": {
      "query": "Michelle Obama",
      "fields": [
        "headline^2",
        "short_description",
        "authors"
      ]
    }
  }
}

# multi match can sometimes mislead
GET news_headlines/_search
{
  "query": {
    "multi_match": {
      "query": "party planning",
      "fields": [
        "headline^2",
        "short_description"
      ]
    }
  }
}

# improve precision with phrase type match
GET news_headlines/_search
{
  "query": {
    "multi_match": {
      "query": "party planning",
      "fields": [
        "headline^2",
        "short_description"
      ],
      "type": "phrase"
    }
  }
}

## combined queries
# bool query
GET name_of_index/_search
{
  "query": {
    "bool": {
      "must": [
        {One or more queries can be specified here. A document MUST match all of these queries to be considered as a hit.}
      ],
      "must_not": [
        {A document must NOT match any of the queries specified here. It it does, it is excluded from the search results.}
      ],
      "should": [
        {A document does not have to match any queries specified here. However, it if it does match, this document is given a higher score.}
      ],
      "filter": [
        {These filters(queries) place documents in either yes or no category. Ones that fall into the yes category are included in the hits. }
      ]
    }
  }
}

# combination of query and aggregation request
GET news_headlines/_search
{
  "query": {
    "match_phrase": {
      "headline": "Michelle Obama"
    }
  },
  "aggs": {
    "category_mentions": {
      "terms": {
        "field": "category",
        "size": 100
      }
    }
  }
}

# must clause
GET news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "headline": "Michelle Obama"
          }
        },
        {
          "match": {
            "category": "POLITICS"
          }
        }
      ]
    }
  }
}

# must_not clause
GET news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "headline": "Michelle Obama"
          }
        }
      ],
      "must_not": [
        {
          "match": {
            "category": "WEDDINGS"
          }
        }
      ]
    }
  }
}

# should clause
GET news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {
        "match_phrase": {
          "headline": "Michelle Obama"
          }
         }
        ],
       "should":[
         {
          "match_phrase": {
            "category": "BLACK VOICES"
          }
        }
      ]
    }
  }
}

# filter clause
GET news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {
        "match_phrase": {
          "headline": "Michelle Obama"
          }
         }
        ],
       "filter":{
          "range":{
             "date": {
               "gte": "2014-03-25",
               "lte": "2016-03-25"
          }
        }
      }
    }
  }
}

# add multiple queries under the should clause in order to improve precision without undermining recall
GET news_headlines/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "headline": "Michelle Obama"
          }
        }
      ],
      "should": [
        {
          "match": {
            "headline": "Becoming"
          }
        },
        {
          "match": {
            "headline": "women"
          }
        },
        {
          "match": {
            "headline": "empower"
          }
        }
      ]
    }
  }
}
```

## [Part 4: Running Aggregations with Elasticsearch and Kibana](https://github.com/LisaHJung/Part-4-Running-Aggregations-with-Elasticsearch-and-Kibana)

### Fix data

```bash
# STEP 1: Create a new index(ecommerce_data) with the following mapping.
PUT ecommerce_data
{
  "mappings": {
    "properties": {
      "Country": {
        "type": "keyword"
      },
      "CustomerID": {
        "type": "long"
      },
      "Description": {
        "type": "text"
      },
      "InvoiceDate": {
        "type": "date",
        "format": "M/d/yyyy H:m"
      },
      "InvoiceNo": {
        "type": "keyword"
      },
      "Quantity": {
        "type": "long"
      },
      "StockCode": {
        "type": "keyword"
      },
      "UnitPrice": {
        "type": "double"
      }
    }
  }
}

# STEP 2: Reindex the data from the original index
POST _reindex
{
  "source": {
    "index": "ecommerce"
  },
  "dest": {
    "index": "ecommerce_data"
  }
}

# STEP 3: Remove the negative values from the field "UnitPrice"
POST ecommerce_data/_delete_by_query
{
  "query": {
    "range": {
      "UnitPrice": {
        "lte": 0
      }
    }
  }
}

# STEP 4: Remove values greater than 500 from the field "UnitPrice"
POST ecommerce_data/_delete_by_query
{
  "query": {
    "range": {
      "UnitPrice": {
        "gte": 500
      }
    }
  }
}
```

### Main queries

```bash
## get info about documents in an index
GET ecommerce_data/_search

## aggregations request
GET Enter_name_of_the_index_here/_search
{
  "aggs": {
    "Name your aggregations here": {
      "Specify the aggregation type here": {
        "field": "Name the field you want to aggregate on here"
      }
    }
  }
}

GET ecommerce_data/_search
{
  "aggs": {
    "by_category": {
      "terms": {
        "field": "Country",
        "size": 100
      }
    }
  }
}

## metric aggregations
#   - sum, min, max, avg, stats, cardinality
GET ecommerce_data/_search
{
  "aggs": {
    "my_agg": {
      "sum": {
        "field": "UnitPrice"
      }
    }
  }
}

# ignore hits
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "my_agg": {
      "stats": {
        "field": "UnitPrice"
      }
    }
  }
}

# limiting the scope of an aggregation
GET ecommerce_data/_search
{
  "size": 0,
  "query": {
    "match": {
      "Country": "Germany"
    }
  },
  "aggs": {
    "avg_unit_price_ge": {
      "avg": {
        "field": "UnitPrice"
      }
    }
  }
}

## bucket aggregation
# date histogram
# histogram
# range
# terms

# date histogram
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "fixed_internal": {
      "date_histogram": {
        "field": "InvoiceDate",
        "fixed_interval": "8h"
      }
    }
  }
}

GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "calendar_interval": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "1M"
      }
    }
  }
}

GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "calendar_interval": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "1M",
        "order": {
          "_key": "desc"
        }
      }
    }
  }
}

# histogram
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_price_interval": {
      "histogram": {
        "field": "UnitPrice",
        "interval": 10
      }
    }
  }
}

GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_price_interval": {
      "histogram": {
        "field": "UnitPrice",
        "interval": 10,
        "order": {
          "_key": "desc"
        }
      }
    }
  }
}

# range
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_custom_price_ranges": {
      "range": {
        "field": "UnitPrice",
        "ranges": [
          {
            "to": 50
          },
          {
            "from": 50,
            "to": 200
          },
          {
            "from": 200
          }
        ]
      }
    }
  }
}

# terms
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "top_5_customers": {
      "terms": {
        "field": "CustomerID",
        "size": 5
      }
    }
  }
}

GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "5_customers_with_lowest_number_of_transactions": {
      "terms": {
        "field": "CustomerID",
        "size": 5,
        "order": {
          "_count": "asc"
        }
      }
    }
  }
}

## combined aggregation (metric + bucket)
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_day": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "day"
      },
      "aggs": {
        "daily_revenue": {
          "sum": {
            "script": {
              "source": "doc['UnitPrice'].value * doc['Quantity'].value"
            }
          }
        }
      }
    }
  }
}

GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_day": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "day"
      },
      "aggs": {
        "daily_revenue": {
          "sum": {
            "script": {
              "source": "doc['UnitPrice'].value * doc['Quantity'].value"
            }
          }
        },
        "number_of_unique_customers_per_day": {
          "cardinality": {
            "field": "CustomerID"
          }
        }
      }
    }
  }
}

# sorting
GET ecommerce_data/_search
{
  "size": 0,
  "aggs": {
    "transactions_per_day": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "day",
        "order": {
          "daily_revenue": "desc"
        }
      },
      "aggs": {
        "daily_revenue": {
          "sum": {
            "script": {
              "source": "doc['UnitPrice'].value * doc['Quantity'].value"
            }
          }
        },
        "number_of_unique_customers_per_day": {
          "cardinality": {
            "field": "CustomerID"
          }
        }
      }
    }
  }
}
```

## [Part 5: Understanding Mapping with Elasticsearch and Kibana](https://github.com/LisaHJung/Part-5-Understanding-Mapping-with-Elasticsearch-and-Kibana)

```bash
# By default (i.e. dynamic indexing), string is indexed twice as test and keyword
# Text field type is designed for full-text searches.
# Keywordfield type is designed for exact searches, aggregations, and sorting.

POST temp_index/_doc
{
  "name": "Pineapple",
  "botanical_name": "Ananas comosus",
  "produce_type": "Fruit",
  "country_of_origin": "New Zealand",
  "date_purchased": "2020-06-02T12:15:35",
  "quantity": 200,
  "unit_price": 3.11,
  "description": "a large juicy tropical fruit consisting of aromatic edible yellow flesh surrounded by a tough segmented skin and topped with a tuft of stiff leaves.These pineapples are sourced from New Zealand.",
  "vendor_details": {
    "vendor": "Tropical Fruit Growers of New Zealand",
    "main_contact": "Hugh Rose",
    "vendor_location": "Whangarei, New Zealand",
    "preferred_vendor": true
  }
}

GET temp_index/_search

GET temp_index/_mapping

## exercise
# Project: Build an app for a client who manages a produce warehouse

# This app must enable users to:
#   - search for produce name, country of origin and description
#   - identify top countries of origin with the most frequent purchase history
#   - sort produce by produce type(Fruit or Vegetable)
#   - get the summary of monthly expense

{
  "name": "Pineapple",
  "botanical_name": "Ananas comosus",
  "produce_type": "Fruit",
  "country_of_origin": "New Zealand",
  "date_purchased": "2020-06-02T12:15:35",
  "quantity": 200,
  "unit_price": 3.11,
  "description": "a large juicy tropical fruit consisting of aromatic edible yellow flesh surrounded by a tough segmented skin and topped with a tuft of stiff leaves.These pineapples are sourced from New Zealand.",
  "vendor_details": {
    "vendor": "Tropical Fruit Growers of New Zealand",
    "main_contact": "Hugh Rose",
    "vendor_location": "Whangarei, New Zealand",
    "preferred_vendor": true
  }
}

# String fileds - Desired Field Type
# - country_of_origin - text and keyword
# - description, name - text
# - produce_type - keyword

# We can define our own mapping and reindex from existing index.
# Also, we can add a runtime field for reporting.
#   - Runtime fields: Schema on read for Elastic
#     https://www.elastic.co/blog/introducing-elasticsearch-runtime-fields
```

## [Part 6: Troubleshooting Beginner Level Elasticsearch Errors](https://github.com/LisaHJung/Part-6-Troubleshooting-Beginner-Level-Elasticsearch-Errors/tree/main)
