locals {
  appname = "rubytrade"

  ec2_ssh_public_key_path = "${pathexpand("~")}/.ssh/kafka_ec2.pub"
}
