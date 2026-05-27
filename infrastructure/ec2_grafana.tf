resource "aws_instance" "grafana_instance" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = "kafka-ec2"
  vpc_security_group_ids = [aws_security_group.grafana_ec2_sg.id]
  subnet_id              = data.aws_subnets.default.ids[0]

  iam_instance_profile = aws_iam_instance_profile.grafana_profile.name

  associate_public_ip_address = true

  tags = {
    Name = "grafana-instance"
  }

  user_data = templatefile("${path.module}/templates/grafana_ec2.tpl", {
  })
}