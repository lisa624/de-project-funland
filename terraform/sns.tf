
#SNS Topic is the “place” (channel) where alert messages are sent
#Without a topic, there’s nowhere to publish alerts
resource "aws_sns_topic" "alerts" {
  name = "funland-lambda-failure-topic"
}

# Subscribe an email address to the SNS topic
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# SNS doesn’t “watch” Lambda failures by itself
# Something needs to detect the failure and trigger the alert
# That “something” is EventBridge (CloudWatch Events).
resource "aws_cloudwatch_event_rule" "lambda_failure_rule" {
  name        = "funland-lambda-failure-alerts"
  description = "Trigger alert when Lambda function fails"

  event_pattern = jsonencode({
    "source": ["aws.lambda"],
    "detail-type": ["Lambda Function Invocation Result - Failure"],
    "detail": {
      "functionName": [
        aws_lambda_function.extract_lambda_handler.function_name,
        aws_lambda_function.transform_lambda_handler.function_name,
        aws_lambda_function.load_lambda_handler.function_name
      ]
    }
  })
}

# Target to send failure events to SNS topic
# The rule detects failures, but must tell AWS what to do when it matches. This is the “target”
resource "aws_cloudwatch_event_target" "send_to_sns" {
  #Attach this target to failure rule
  rule      = aws_cloudwatch_event_rule.lambda_failure_rule.name
  target_id = "SendToSNS"
  #The target is SNS topic
  arn       = aws_sns_topic.alerts.arn
}