data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  name        = "lambda-demo"
  region      = data.aws_region.current.name
  environment = "dev"

  default_bucket = {
    to_update_acl = false
    name          = "${local.name}-${data.aws_caller_identity.current.account_id}-${local.region}"
  }

  tags = {
    Name        = local.name
    Environment = local.environment
  }
}
