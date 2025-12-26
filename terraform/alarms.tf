
# ############################

# resource "aws_cloudwatch_log_group" "extract_log_group" {
#   name              = "/aws/lambda/${aws_lambda_function.extract_lambda_handler.function_name}"
#   retention_in_days = 14
# }

# resource "aws_cloudwatch_log_group" "transform_log_group" {
#   name              = "/aws/lambda/${aws_lambda_function.transform_lambda_handler.function_name}"
#   retention_in_days = 14
# }

# resource "aws_cloudwatch_log_group" "load_log_group" {
#   name              = "/aws/lambda/${aws_lambda_function.load_lambda_handler.function_name}"
#   retention_in_days = 14
# }

# ############################
# # 1) Log metric filters for "ERROR" text in logs
# #    (This creates custom metrics you can alarm on if you want)
# ############################

# resource "aws_cloudwatch_log_metric_filter" "error_filter_extract" {
#   name           = "ErrorFilter-extract"
#   log_group_name = aws_cloudwatch_log_group.extract_log_group.name
#   pattern        = "ERROR"

#   metric_transformation {
#     name      = "ErrorCountExtract"
#     namespace = "lambda_errors"
#     value     = "1"
#   }

#   depends_on = [
#     aws_cloudwatch_log_group.extract_log_group
#   ]
# }

# resource "aws_cloudwatch_log_metric_filter" "error_filter_transform" {
#   name           = "ErrorFilter-transform"
#   log_group_name = aws_cloudwatch_log_group.transform_log_group.name
#   pattern        = "ERROR"

#   metric_transformation {
#     name      = "ErrorCountTransform"
#     namespace = "lambda_errors"
#     value     = "1"
#   }

#   depends_on = [
#     aws_cloudwatch_log_group.transform_log_group
#   ]
# }

# resource "aws_cloudwatch_log_metric_filter" "error_filter_load" {
#   name           = "ErrorFilter-load"
#   log_group_name = aws_cloudwatch_log_group.load_log_group.name
#   pattern        = "ERROR"

#   metric_transformation {
#     name      = "ErrorCountLoad"
#     namespace = "lambda_errors"
#     value     = "1"
#   }

#   depends_on = [
#     aws_cloudwatch_log_group.load_log_group
#   ]
# }

# ############################
# # 2) CloudWatch alarms (Lambda service-level Errors metric)
# #    This alarms when AWS reports Lambda invocation errors.
# ############################

# resource "aws_cloudwatch_metric_alarm" "lambda_extract_alarm" {
#   alarm_name          = "lambda-extract-error-alarm"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Alarm when Lambda Extract has any invocation errors"
#   alarm_actions       = [aws_sns_topic.alerts.arn]

#   dimensions = {
#     FunctionName = aws_lambda_function.extract_lambda_handler.function_name
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "lambda_transform_alarm" {
#   alarm_name          = "lambda-transform-error-alarm"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Alarm when Lambda Transform has any invocation errors"
#   alarm_actions       = [aws_sns_topic.alerts.arn]

#   dimensions = {
#     FunctionName = aws_lambda_function.transform_lambda_handler.function_name
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "lambda_load_alarm" {
#   alarm_name          = "lambda-load-error-alarm"
#   comparison_operator = "GreaterThanThreshold"
#   evaluation_periods  = 1
#   metric_name         = "Errors"
#   namespace           = "AWS/Lambda"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 0
#   alarm_description   = "Alarm when Lambda Load has any invocation errors"
#   alarm_actions       = [aws_sns_topic.alerts.arn]

#   dimensions = {
#     FunctionName = aws_lambda_function.load_lambda_handler.function_name
#   }
# }


