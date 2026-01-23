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
