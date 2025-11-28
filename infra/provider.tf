terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3.0"
}

provider "aws" {
  alias = "CDE_Chikodi_Obu_5"
  region = var.aws_region
}

provider "aws" {
  alias = "root_user"
  region = var.aws_region

  profile = "chikodi-main"
}
