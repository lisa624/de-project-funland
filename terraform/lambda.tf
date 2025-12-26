
#Creates a Terraform data object that produces a zip file
data "archive_file" "extract_lambda" {
  type             = "zip"
  source_file      = "${path.module}/../src/lambda_handler/extract.py"
  output_path      = "${path.module}/../build/extract_function.zip"
  output_file_mode = "0666"
}

#zip the transform lambda code 
data "archive_file" "transform_lambda" {
  type             = "zip"
  source_file      = "${path.module}/../src/lambda_handler/transform.py"
  output_path      = "${path.module}/../build/transform_function.zip"
  output_file_mode = "0666"
}

#zip the load lambda code
data "archive_file" "load_lambda" {
  type             = "zip"
  source_file      = "${path.module}/../src/lambda_handler/load.py"
  output_path      = "${path.module}/../build/load_function.zip"
  output_file_mode = "0666"
}


#################################
#Upload code zips to S3
#################################

#AWS will fetch the zip from S3
#terraform must upload it there first
resource "aws_s3_object" "extract_zip" {
  bucket = aws_s3_bucket.layer_bucket.bucket
  key    = "lambda/extract_function.zip"
  source = data.archive_file.extract_lambda.output_path

  # helps force updates correctly
  etag = filemd5(data.archive_file.extract_lambda.output_path)
}

resource "aws_s3_object" "transform_zip" {
  bucket = aws_s3_bucket.layer_bucket.bucket
  key    = "lambda/transform_function.zip"
  source = data.archive_file.transform_lambda.output_path
  etag   = filemd5(data.archive_file.transform_lambda.output_path)
}

resource "aws_s3_object" "load_zip" {
  bucket = aws_s3_bucket.layer_bucket.bucket
  key    = "lambda/load_function.zip"
  source = data.archive_file.load_lambda.output_path
  etag   = filemd5(data.archive_file.load_lambda.output_path)
}


#################################
# 3) Upload the layer zip to S3
#################################
# This replaces your old "null_resource pip install" approach.
# Build this zip in CI/CD (recommended), then Terraform just uploads it.

resource "aws_s3_object" "etl_layer_zip" {
  bucket = aws_s3_bucket.layer_bucket.bucket
  key    = "layers/etl_layer.zip"
  source = "${path.module}/../build/etl_layer.zip"

  # If the file doesn't exist, terraform apply will fail (good - it forces a proper build step in CI)
  etag = filemd5("${path.module}/../build/etl_layer.zip")
}

resource "aws_lambda_layer_version" "etl_layer" {
  layer_name          = "etl_layer"
  compatible_runtimes = [var.python_runtime]

  s3_bucket = aws_s3_object.etl_layer_zip.bucket
  s3_key    = aws_s3_object.etl_layer_zip.key
}


############################
# 4) Lambda functions
############################

resource "aws_lambda_function" "extract_lambda_handler" {
  function_name = var.lambda_extract
  role          = aws_iam_role.lambda_role.arn

  handler = "extract.lambda_handler"
  runtime = var.python_runtime

  timeout     = 900
  memory_size = 3000

  # S3 deployment (CI/CD friendly)
  s3_bucket = aws_s3_object.extract_zip.bucket
  s3_key    = aws_s3_object.extract_zip.key

  # Detect code changes properly
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256

  layers = [
    aws_lambda_layer_version.etl_layer.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17"
  ]

  environment {
    variables = {
      S3_INGESTION_BUCKET = aws_s3_bucket.ingestion_bucket.bucket
      DB_SECRET_NAME      = var.db_credentials
    }
  }
}


resource "aws_lambda_function" "transform_lambda_handler" {
  function_name = var.lambda_transform
  role          = aws_iam_role.lambda_role.arn

  handler = "transform.lambda_handler"
  runtime = var.python_runtime

  timeout     = 900
  memory_size = 3000

  s3_bucket = aws_s3_object.transform_zip.bucket
  s3_key    = aws_s3_object.transform_zip.key

  source_code_hash = data.archive_file.transform_lambda.output_base64sha256

  layers = [
    aws_lambda_layer_version.etl_layer.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17"
  ]

  environment {
    variables = {
      S3_INGESTION_BUCKET = aws_s3_bucket.ingestion_bucket.bucket
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed_bucket.bucket
    }
  }
}


resource "aws_lambda_function" "load_lambda_handler" {
  function_name = var.lambda_load
  role          = aws_iam_role.lambda_role.arn

  handler = "load.lambda_handler"
  runtime = var.python_runtime

  timeout     = 900
  memory_size = 3000

  s3_bucket = aws_s3_object.load_zip.bucket
  s3_key    = aws_s3_object.load_zip.key

  source_code_hash = data.archive_file.load_lambda.output_base64sha256

  layers = [
    aws_lambda_layer_version.etl_layer.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17"
  ]

  environment {
    variables = {
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed_bucket.bucket
    }
  }
}
