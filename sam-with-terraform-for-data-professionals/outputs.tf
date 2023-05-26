output "transformer_function_arn" {
  description = "ARN of the transformer lambda function"
  value       = module.transformer.lambda_function_arn
}

output "fastavro_layer_arn" {
  description = "ARN of the fastavro lambda layer"
  value       = module.fastavro.lambda_layer_arn
}

output "default_bucket_arn" {
  description = "ARN of the fastavro lambda layer"
  value       = aws_s3_bucket.default_bucket.arn
}
