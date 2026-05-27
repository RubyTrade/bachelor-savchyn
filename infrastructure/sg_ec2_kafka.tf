resource "aws_security_group" "kafka_ec2_sg" {
    name = "${local.appname}-kafka-ec2-sg"
    description = "Allow SSH and Kafka ports"

    ingress {
        description = "SSH"
        from_port = 22
        to_port = 22
        protocol = "tcp"
        cidr_blocks = [ "0.0.0.0/0" ]
    }

    ingress {
        description = "kafka"
        from_port = 9092
        to_port = 9092
        protocol = "tcp"
        cidr_blocks = [
            "10.0.0.0/8",
            "172.0.0.0/8",
            "192.168.0.0/16"
        ]
    }
    

    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        cidr_blocks = [ "0.0.0.0/0" ]
    }
  
}