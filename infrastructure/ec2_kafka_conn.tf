resource "aws_instance" "kafka_connect_instance" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.kafka_instance_kp.key_name
  vpc_security_group_ids = [aws_security_group.kafka_ec2_sg.id]
  subnet_id              = data.aws_subnets.default.ids[0]

  iam_instance_profile = aws_iam_instance_profile.kafka_connect_profile.name

  associate_public_ip_address = true

  tags = {
    Name = "kafka-S3-sink-connector"
  }

  user_data = templatefile("${path.module}/templates/kafka_connector_ec2.tpl", {
    kafka_version = var.kafka_version
    kafka_bootstrap = aws_instance.kafka_instance.private_ip
    s3_bucket = aws_s3_bucket.kafka_sink.bucket
    aws_region = var.aws_region
  })

}
