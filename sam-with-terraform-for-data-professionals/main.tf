module "fastavro" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 4.0"

  create_layer = true

  layer_name          = "fastavro-layer-py3"
  description         = "fastavro lambda layer"
  compatible_runtimes = ["python3.7", "python3.8", "python3.9", "python3.10"]

  source_path = "./lambda/fastavro"
  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.default_bucket.id
  s3_prefix   = "packages/"

  tags = local.tags
}

module "transformer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 4.0"

  function_name = "${local.name}-transformer"
  description   = "transform csv file into parquet/avro files and save to s3"
  handler       = "app.lambda_handler"
  runtime       = "python3.8"
  timeout       = 20
  memory_size   = 256

  source_path = "./lambda/transform"
  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.default_bucket.id
  s3_prefix   = "packages/"

  layers = [
    module.fastavro.lambda_layer_arn,
    "arn:aws:lambda:ap-southeast-2:336392948345:layer:AWSSDKPandas-Python38:7"
  ]

  allowed_triggers = {
    AllowExecutionFromS3Bucket = {
      service    = "s3"
      source_arn = aws_s3_bucket.default_bucket.arn
    }
  }

  attach_policies    = true
  policies           = [aws_iam_policy.transformer_lambda_permission.arn]
  number_of_policies = 1

  tags = local.tags
}

module "s3_notification" {
  source  = "terraform-aws-modules/s3-bucket/aws//modules/notification"
  version = "~> 3.0"

  bucket = aws_s3_bucket.default_bucket.id

  eventbridge = true

  lambda_notifications = {
    lambda1 = {
      function_arn  = module.transformer.lambda_function_arn
      function_name = module.transformer.lambda_function_name
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "input/"
      filter_suffix = ".csv"
    }
  }
}

resource "aws_iam_policy" "transformer_lambda_permission" {
  name = "${local.name}-lambda-permission"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListObjectsInBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.default_bucket.arn
      },
      {
        Sid      = "AllObjectActions"
        Effect   = "Allow"
        Action   = ["s3:*Object"]
        Resource = "${aws_s3_bucket.default_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_s3_bucket" "default_bucket" {
  bucket = local.default_bucket.name

  force_destroy = true

  tags = local.tags
}

resource "aws_s3_bucket_acl" "default_bucket" {
  count  = local.default_bucket.to_update_acl ? 1 : 0
  bucket = aws_s3_bucket.default_bucket.id
  acl    = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "default_bucket" {
  bucket = aws_s3_bucket.default_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
