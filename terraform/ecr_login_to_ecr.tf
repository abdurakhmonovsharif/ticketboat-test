resource "null_resource" "login_to_ecr" {
  depends_on = [aws_ecr_repository.ecr_repository]
  triggers = {
    current_timestamp = var.current_timestamp
    ecr_repo = aws_ecr_repository.ecr_repository.repository_url
  }

  provisioner "local-exec" {
    command = <<EOF
    set -e # Exit immediately if a command exits with a non-zero status.
    cd ..

    echo "Log into AWS ECR Container Repository"
    aws ecr get-login-password \
      --region ${data.aws_region.current.name} | \
      docker login \
        --username AWS \
        --password-stdin ${aws_ecr_repository.ecr_repository.repository_url}
    EOF
  }
}
