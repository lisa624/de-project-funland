
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = var.aws_region
}


# ---------------
# Lambda IAM Role
# ---------------

# Define role: allows lambda to assume this role
 data "aws_iam_policy_document" "trust_policy" {
   statement {
     effect = "Allow"
     principals {
       type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

     actions = ["sts:AssumeRole"]
  }
}

# Create the role for lambda
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "lambda-role-"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------------
# Lambda IAM Policy to S3
# ------------------------------

# Define S3 document
data "aws_iam_policy_document" "s3_data_policy_doc" {
  # Allow listing buckets (ListBucket must be on the bucket ARN)
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.ingestion_bucket.arn,
      aws_s3_bucket.processed_bucket.arn
    ]
  }

  # Allow read/write objects (object actions must be on arn/*)
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.processed_bucket.arn}/*"
    ]
  }
}
# Create S3 policy
resource "aws_iam_policy" "s3_read_and_write_policy" {
  name_prefix = "s3-policy-lambda-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json 
}

# Attach s3 policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_read_and_write_policy.arn
}

# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define cw doc. 
data "aws_iam_policy_document" "cw_document" {
  # CloudWatch Logs permissions
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:*"]
  }

  # Put custom metrics (resource-level restrictions are not supported for PutMetricData, so "*")
  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

# Create CloudeWatch policy
resource "aws_iam_policy" "cw_policy" {
  name   = "cw-lambda"
  policy = data.aws_iam_policy_document.cw_document.json
}

# Attach the cloudeWatch policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# ------------------------------
# Lambda needs permission to use SSM to store last updated
# ------------------------------

# define ssm policy
data "aws_iam_policy_document" "ssm_lambda_policy_documentum"{
  statement {
    effect = "Allow"
    actions = [
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "ssm:GetParameterHistory"
        ]
    resources = ["arn:aws:ssm:${local.region}:${local.account_id}:parameter/last_checked"]
  }
}

# create ssm the policy
resource "aws_iam_policy" "ssm_lambda_policy" {
  name = "lambda-access-ssm-policy"
  description = "Allow lambda to read/write the last_checked SSM parameter"
  policy = data.aws_iam_policy_document.ssm_lambda_policy_documentum.json
}

# attach ssm policy to lambda role
resource "aws_iam_role_policy_attachment" "ssm_lambda_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.ssm_lambda_policy.arn
}


# -----------------------------
# Lambda permission to read db credentials from Secrets Manager ("db_creds")
# -----------------------------
# Lambdas should READ secrets, not create/update them.
#-----------------------------


data "aws_iam_policy_document" "secretsmanager_lambda_policy_document" {
  statement { 
    effect = "Allow"
    actions = [
    
      "secretsmanager:PutSecretValue",
      "secretsmanager:DescribeSecret"

    ]
    # Secret ARNs end with a random suffix; using db_creds* is common
    resources = [
      "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:db_creds*"
    ]
  }
}

# Create Iam policy for secret manager
resource "aws_iam_policy" "secretsmanager_lambda_policy" {
  name = "lambda-secretmanager-access"
  policy = data.aws_iam_policy_document.secretsmanager_lambda_policy_document.json
}

# Attach secret manager policy to lambda
resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.secretsmanager_lambda_policy.arn
} 

# ------------------------------
# IAM role for Step Function to invoke Lambda
# ------------------------------

# Data for Step Function doc role
data "aws_iam_policy_document" "sf_role_document" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# Create State Machine role
resource "aws_iam_role" "step_function_role" {
  name_prefix        = "role-${var.step_function}-"
  assume_role_policy = data.aws_iam_policy_document.sf_role_document.json
}

# Define policy that allows Step Function to invoke lambdas
data "aws_iam_policy_document" "step_functions_document" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      aws_lambda_function.extract_lambda_handler.arn,
      aws_lambda_function.transform_lambda_handler.arn,
      aws_lambda_function.load_lambda_handler.arn
      ]    
  }
}

# Create IAM policy for Step Function
resource "aws_iam_policy" "step_functions_policy" {
  name = "sf-invoke-lambda-"
  policy = data.aws_iam_policy_document.step_functions_document.json
}

# attach the cw policy to the lambda role
resource "aws_iam_role_policy_attachment" "lambda_sf_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}


# SNS | sending email alerts
# ------------------------------
# Lambdas & Step Functions only need Publish (Terraform should create topic/subscription).

# define the policy
data "aws_iam_policy_document" "sns_publish_document" {
  statement {
    effect = "Allow"
    actions = [ 
      "sns:Publish"
     ]
    resources = [ aws_sns_topic.alerts.arn ]
  }
}

# create sns policy
resource "aws_iam_policy" "sns_publish_policy" {
  name = "sns-publish-"
  policy = data.aws_iam_policy_document.sns_publish_document.json
}

# attach sns policy to lambda
resource "aws_iam_role_policy_attachment" "sns_attached_lambda" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}

# attach sns policy to step function
resource "aws_iam_role_policy_attachment" "sns_attached_sf" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}

# allow eventbridge to send SNS message
data "aws_iam_policy_document" "sns_topic_policy_document" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = [
      "SNS:Publish"
    ]

    resources = [
      aws_sns_topic.alerts.arn
    ]

    condition {
      test     = "ArnEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudwatch_event_rule.lambda_failure_rule.arn] 
  }
}


resource "aws_sns_topic_policy" "alerts_topic_policy" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_topic_policy_document.json

}



# ------------------------------
# IAM Policy for Scheduler to invoke step function
# ------------------------------
 
# Data for role
data "aws_iam_policy_document" "scheduler_role_document" {

  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}


# Create the role for sceduler

data "aws_iam_policy_document" "scheduler_role_document" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}


resource "aws_iam_role" "scheduler_role" {
  name        = "scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_role_document.json
}


data "aws_iam_policy_document" "scheduler_policy_document" {
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.sfn_state_machine.arn]
  }
}


resource "aws_iam_policy" "scheduler_policy" {
  name = "scheduler-policy"
  policy = data.aws_iam_policy_document.scheduler_policy_document.json
}

# Attach scheduler policy to step function
resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}

