
# Create an S3 bucket
resource "aws_s3_bucket" "my_bucket" {
  bucket = "crs-landing-bucket-125"
}

provider "aws" {
  region  = "eu-west-1"
}

# Block public access
resource "aws_s3_bucket_public_access_block" "public_access_block" {
  bucket = aws_s3_bucket.my_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
