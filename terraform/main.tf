terraform {
  required_providers {
    aws={
      source="hashicorp/aws"
      version="~> 5.0"
    }
    
  }
  backend "s3" {
    bucket = "funland-terraform-configure-1-backend-45"
    key    = "folder/terraform.tfstate"
    region = "eu-west-2"
  }

}

provider "aws" {
  region = "eu-west-2"
}
