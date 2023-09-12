## connectors
# camel dynamodb sink
resource "aws_mskconnect_connector" "opensearch_sink" {
  name = "${local.name}-ad-tech-sink"

  kafkaconnect_version = "2.7.1"

  capacity {
    provisioned_capacity {
      mcu_count    = 1
      worker_count = 1
    }
  }

  connector_configuration = {
    # connector configuration
    "connector.class"                = "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
    "tasks.max"                      = "2",
    "topics"                         = "impressions,clicks",
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable"   = false,
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable" = false,
    # opensearch sink configuration
    "connection.url"                  = "",
    "connection.username"             = "",
    "connection.password"             = "",
    "schema.ignore"                   = true,
    "key.ignore"                      = true,
    "type.name"                       = "_doc",
    "behavior.on.malformed.documents" = "fail",
    "behavior.on.null.values"         = "ignore",
    "behavior.on.version.conflict"    = "ignore",
    # dead-letter-queue configuration
    "errors.deadletterqueue.topic.name"               = "ad-tech-dl",
    "errors.tolerance"                                = "all",
    "errors.deadletterqueue.context.headers.enable"   = true,
    "errors.deadletterqueue.topic.replication.factor" = "1",
    # single message transforms
    "transforms"                          = "insertTS,formatTS",
    "transforms.insertTS.type"            = "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.insertTS.timestamp.field" = "created_at",
    "transforms.formatTS.type"            = "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.formatTS.format"          = "yyyy-MM-dd HH:mm:ss",
    "transforms.formatTS.field"           = "created_at",
    "transforms.formatTS.target.type"     = "string"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam

      vpc {
        security_groups = [aws_security_group.msk.id]
        subnets         = module.vpc.private_subnets
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "IAM"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.opensearch_sink.arn
      revision = aws_mskconnect_custom_plugin.opensearch_sink.latest_revision
    }
  }

  log_delivery {
    worker_log_delivery {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.opensearch_sink.name
      }
      s3 {
        enabled = true
        bucket  = aws_s3_bucket.default_bucket.id
        prefix  = "logs/msk/connect/opensearch-sink"
      }
    }
  }

  service_execution_role_arn = aws_iam_role.kafka_connector_role.arn

  depends_on = [
    aws_mskconnect_connector.msk_data_generator
  ]
}

resource "aws_mskconnect_custom_plugin" "opensearch_sink" {
  name         = "${local.name}-opensearch-sink"
  content_type = "ZIP"

  location {
    s3 {
      bucket_arn = aws_s3_bucket.default_bucket.arn
      file_key   = aws_s3_object.opensearch_sink.key
    }
  }
}

resource "aws_s3_object" "opensearch_sink" {
  bucket = aws_s3_bucket.default_bucket.id
  key    = "plugins/opensearch-connector.zip"
  source = "connectors/opensearch-connector.zip"

  etag = filemd5("connectors/opensearch-connector.zip")
}

resource "aws_cloudwatch_log_group" "opensearch_sink" {
  name = "/msk/connect/opensearch-sink"

  retention_in_days = 1

  tags = local.tags
}

# msk data generator
resource "aws_mskconnect_connector" "msk_data_generator" {
  name = "${local.name}-ad-tech-source"

  kafkaconnect_version = "2.7.1"

  capacity {
    provisioned_capacity {
      mcu_count    = 1
      worker_count = 1
    }
  }

  connector_configuration = {
    # connector configuration
    "connector.class"                = "com.amazonaws.mskdatagen.GeneratorSourceConnector",
    "tasks.max"                      = "2",
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable"   = false,
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable" = false,
    # msk data generator configuration
    "genkp.impressions.with"                        = "#{Code.isbn10}"
    "genv.impressions.bid_id.with"                  = "#{Code.isbn10}"
    "genv.impressions.campaign_id.with"             = "#{Code.isbn10}"
    "genv.impressions.creative_details.with"        = "#{Color.name}"
    "genv.impressions.country_code.with"            = "#{Address.countryCode}"
    "genkp.clicks.with"                             = "#{Code.isbn10}"
    "genv.clicks.correlation_id.sometimes.matching" = "impressions.value.bid_id"
    "genv.clicks.correlation_id.sometimes.with"     = "NA"
    "genv.clicks.tracker.with"                      = "#{Lorem.characters '15'}"
    "global.throttle.ms"                            = "500"
    "global.history.records.max"                    = "1000"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam

      vpc {
        security_groups = [aws_security_group.msk.id]
        subnets         = module.vpc.private_subnets
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "IAM"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.msk_data_generator.arn
      revision = aws_mskconnect_custom_plugin.msk_data_generator.latest_revision
    }
  }

  log_delivery {
    worker_log_delivery {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_data_generator.name
      }
      s3 {
        enabled = true
        bucket  = aws_s3_bucket.default_bucket.id
        prefix  = "logs/msk/connect/msk-data-generator"
      }
    }
  }

  service_execution_role_arn = aws_iam_role.kafka_connector_role.arn
}

resource "aws_mskconnect_custom_plugin" "msk_data_generator" {
  name         = "${local.name}-msk-data-generator"
  content_type = "JAR"

  location {
    s3 {
      bucket_arn = aws_s3_bucket.default_bucket.arn
      file_key   = aws_s3_object.msk_data_generator.key
    }
  }
}

resource "aws_s3_object" "msk_data_generator" {
  bucket = aws_s3_bucket.default_bucket.id
  key    = "plugins/msk-data-generator.jar"
  source = "connectors/msk-data-generator.jar"

  etag = filemd5("connectors/msk-data-generator.jar")
}

resource "aws_cloudwatch_log_group" "msk_data_generator" {
  name = "/msk/connect/msk-data-generator"

  retention_in_days = 1

  tags = local.tags
}

## IAM permission
resource "aws_iam_role" "kafka_connector_role" {
  name = "${local.name}-connector-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "kafkaconnect.amazonaws.com"
        }
      },
    ]
  })
  managed_policy_arns = [
    aws_iam_policy.kafka_connector_policy.arn
  ]
}

resource "aws_iam_policy" "kafka_connector_policy" {
  name = "${local.name}-connector-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "PermissionOnCluster"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:cluster/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnTopics"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:topic/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnGroups"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:group/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnDataBucket"
        Action = [
          "s3:ListBucket",
          "s3:*Object"
        ]
        Effect = "Allow"
        Resource = [
          "${aws_s3_bucket.default_bucket.arn}",
          "${aws_s3_bucket.default_bucket.arn}/*"
        ]
      },
      {
        Sid = "LoggingPermission"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}
