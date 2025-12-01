resource "aws_iam_role" "snowflake_role" {
  provider = aws.root_user 
  name = "snowflake-s3-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          "AWS" : var.snowflake_iam_user_arn
        }
        "Condition": {
          "StringEquals": {
            "sts:ExternalId": var.snowflake_external_id
        }
      }
      },
    ]
  })

  tags = {
    Environment = "dev"
    Owner = "chik0di"
    Managed_by = "Terraform"
  }
}

resource "aws_iam_role_policy" "snowflake_inline_policy" {
  provider = aws.root_user
  name = "snowflake-s3-inline-policy"
  role = aws_iam_role.snowflake_role.id

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
          Effect   = "Allow"
          Resource = [
            "${var.aws_s3_bucket_arn}",
            "${var.aws_s3_bucket_arn}/*"
          ]
        },
      ]
    })
  }