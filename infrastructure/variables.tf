variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "availability_zone" {
  type = string
  default = "us-east-1d"
}

variable "instance_type" {
  type = string
  default = "t3.small"
}

variable "kafka_version" {
  type = string
  default = "3.7.1"
}

variable "glue_job_name" {
  type    = string
  default = "trading-lakehouse-pipeline"
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 2
}

variable "glue_daily_schedule" {
  type    = string
  default = "cron(0 2 * * ? *)"
}

variable "glue_enable_daily_trigger" {
  type    = bool
  default = true
}
