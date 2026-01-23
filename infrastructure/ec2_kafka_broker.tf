resource "aws_instance" "kafka_instance" {
    ami = data.aws_ami.ubuntu.id
    instance_type = var.instance_type
    key_name = aws_key_pair.kafka_instance_kp.key_name
    vpc_security_group_ids = [aws_security_group.kafka_ec2_sg.id]
    subnet_id = data.aws_subnets.default.ids[0]

    associate_public_ip_address = true

    tags = {
        Name = "kafka-node-1"
    }

  user_data = templatefile("${path.module}/templates/kafka_ec2_broker.tpl",  {
    kafka_version = var.kafka_version
  })

}

resource "aws_key_pair" "kafka_instance_kp" {
  key_name = "kafka-ec2"
  public_key = file(local.ec2_ssh_public_key_path)
}
