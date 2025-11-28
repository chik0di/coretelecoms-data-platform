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
          "AWS" : "arn:aws:iam::714551764970:user/3sx71000-s"
        }
        "Condition": {
          "StringEquals": {
            "sts:ExternalId": "RU04201_SFCRole=4_+3jpzChFveqGCVBsJe8fCTL9ii4="
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
            "arn:aws:s3:::core-telecoms-dev-bronze-layer",
            "arn:aws:s3:::core-telecoms-dev-bronze-layer/*"
          ]
        },
      ]
    })
  }