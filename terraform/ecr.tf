resource "aws_ecr_repository" "ecr_repository" {
  name = "${var.app_ident}_repository"
}

variable "code_hash_file" {
  description = "Filename of the code hash file"
  type        = string
}

resource "null_resource" "push_image" {
  depends_on = [aws_ecr_repository.ecr_repository, null_resource.login_to_ecr]
  triggers = {
    code_hash = filemd5(var.code_hash_file)
    ecr_repo = aws_ecr_repository.ecr_repository.repository_url
    force = 2
  }

  provisioner "local-exec" {
    command = <<EOF
    set -e # Exit immediately if a command exits with a non-zero status.
    cd ..

    echo "Running docker build: ${path.cwd}"

    # For ARM Mac: docker buildx build --platform linux/amd64 \
    # For Non-ARM (bitbucket,windows laptops): docker build \
    echo "Build the Docker Image"
    docker build \
      -t ${aws_ecr_repository.ecr_repository.repository_url}:${self.triggers.code_hash} \
      -t ${aws_ecr_repository.ecr_repository.repository_url}:latest \
      .

    echo "Push Docker Image to AWS ECR Container Repository"
    docker push ${aws_ecr_repository.ecr_repository.repository_url}:${self.triggers.code_hash}
    docker push ${aws_ecr_repository.ecr_repository.repository_url}:latest
    sleep 10
    EOF
  }
}
