resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze"

  tags = {
  Project     = var.project_name
  Owner       = "chik0di"
  Environment = var.environment
  Layer       = "bronze"
    }
}

resource "aws_s3_bucket" "root_target_bucket" {
  provider = aws.root_user
  bucket = "${var.project_name}-${var.environment}-bronze-layer"

  tags = {
  Project     = var.project_name
  Owner       = "chik0di"
  Environment = var.environment
  Layer       = "bronze"
    }
}
