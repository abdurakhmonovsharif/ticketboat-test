# Python AWS Lambda Backend API for the NRD Tech Admin App

### Technology Stack
* Python 3.10
* Docker
* Terraform

---

### Getting Started with this Template
1. Make sure Docker Desktop is running on your machine
2. [Clone the project](#clone-the-project)
3. [Initialize a new git repo for your project](#initialize-a-new-git-repo-for-your-project)
4. Create a Project in Bitbucket and Push this project to the main branch

### Configuring the Project and CI/CD
1. [Set up Firebase Account](#set-up-firebase-account)
2. [Store the Firebase Credentials in AWS Secrets](#store-the-firebase-credentials-in-aws-secrets)
3. [Set up bitbucket OIDC pipeline deployment](#set-up-bitbucket-oidc-pipeline-deployment)
4. [Configure Project Settings](#configure-project-settings)
5. [Enable Bitbucket Pipeline](#enable-bitbucket-pipeline)
6. [(Optional) Configure Environment Secrets](#optional-configure-environment-secrets)

### Running the project locally
1. Use VSCode - https://code.visualstudio.com/download
2. Start the devcontainer in VSCode
3. [Create a .env file](#create-env-file)
4. [Run the project](#run-the-project)

### Deploy the project to Staging
1. [Deploy to Staging](#deploy-to-staging)

### Deploy the project to Production
1. [Deploy to Production](#deploy-to-production)

---

## How to connect to the Ticketboat CodeArtifact to get pip dependencies
```
AWS_PROFILE=<ticketboat profile> aws --region us-east-1 codeartifact login --tool pip --domain ticketboat --domain-owner 317822790556 --repository ticketboat-py-repo

Example:
AWS_PROFILE=ticketboat aws --region us-east-1 codeartifact login --tool pip --domain ticketboat --domain-owner 317822790556 --repository ticketboat-py-repo
```

### Clone the project
```
git clone https://<your user>@bitbucket.org/nrd-tech/aws_lambda_eventbridge.git my_project
git clone https://<your user>@bitbucket.org/nrd-tech/python-aws-lambda-basic-api.git my-project
cd my_project
```

### Initialize a new git repo for your project
```
rm -fR .git venv .idea
git init
git branch -m main
git add .
git commit -m 'init'
```

---

### Set up Firebase Account
This project uses Firebase for the Authentication backend
* Go to: https://console.firebase.google.com/
* Click: Create a Project
  * Enter Project Name, Accept Terms, Continue
  * Enable Google Analytics, Continue
* Once the project is created and you are on the Project Overview:
  * Click Build -> Authentication
  * Click Get Started
    * Enable desired Sign-in Methods (Email/Password, Google, Apple, Microsoft, etc.)
      * Google
        * Leave the optional settings empty. Google will auto-configure them for you
      * [Setup Microsoft](#set-up-microsoft-login-support-for-firebase)
    * Go to Settings Tab
      * Set user account linking to:
        * Create multiple accounts for each identity provider
      * Add your admin domain to the list of Authorized domains
        * be sure to include the Staging and Production environments
  * Click on the Gear next to Project Overview on the top left
    * Click on the Service accounts tab
      * Under Firebase Admin SDK
        * Click on the Python radio button
        * Click the Generate new private key
          * Click Generate key button
          * Save the downloaded file in a safe place (we will store this in AWS Secrets as documented below)
  * Click Build -> Realtime Database
    * Click Create Database
      * Choose a location closest to your AWS Region
      * Choose Start in locked mode
      * Click Enable
    * Save the database url link to be used in configuration below
  * Click on Gear next to Project Overview on the top left
    * Create a Web App by clicking the </> button in the Your Apps section
      * Enter a name
      * Leave the Firebase Hosting unchecked
      * Register app
      * Save the npm code in a safe place so it can be referenced later during the config of the frontend

### Set up Microsoft Login support for Firebase
- Navigate to your Microsoft Entra ID system in Azure Portal
- Click New App Registration
- Configure the new app
- Copy out the Application ID and put in the Firebase config
- Click Certificates & secrets
  - Create a New client secret
  - Copy the secret Value over to Firease config

### Store the Firebase Credentials in AWS Secrets
* Navigate to the AWS Secrets Manager
* Click Store a new secret
* Click Other type of secret
* In the Key/value method
  * Key: firebase_credentials
  * Value: <Paste the contents of the downloaded json file from the Firebase Account Setup>
* Click Next
* Name the secret: prod/firebase_credentials
* Click Next
* Click Next
* Click Store

### Set up bitbucket OIDC pipeline deployment
* You must have previously run the NRD-Tech Terraform Bootstrap template to link AWS to Bitbucket with a Role
  * https://bitbucket.org/nrd-tech/bitbucket-tf-account-bootstrap/src/main/
  * You should have an AWS Role ARN from this bootstrap as well as the terraform state bucket

### Configure Project Settings
* Edit .env.global
  * Each config is a little different per application but at a minimum you will need to change:
    * APP_IDENT_WITHOUT_ENV
    * TERRAFORM_STATE_BUCKET
      * This comes from the Bitbucket OIDC setup and is used for automated deployment to AWS
    * AWS_DEFAULT_REGION
    * AWS_ROLE_ARN
      * This comes from the Bitbucket OIDC setup and is used for automated deployment to AWS
* Edit .env.staging AND .env.production
  * API_ROOT_DOMAIN
  * API_DOMAIN
* Commit your changes to git
```
git add .
git commit -a -m 'updated config'
```

### Enable Bitbucket Pipeline
* Push your git project up into a new Bitbucket project
* Navigate to your project on Bitbucket
  * Click Repository Settings
  * Click Pipelines->Settings
    * Click Enable Pipelines

### (Optional) Configure Environment Secrets
* Navigate to your project on Bitbucket
  * Click Repository Settings
  * Click Repository variables
  * Enter any variables in here that you want as secrets in environment variables when you deploy

---

### Create .env file
* Create .env (from the example below) in root directory of the project
* Change the AWS_PROFILE field to match a ~/.aws/credentials profile that has the appropriate AWS permissions

### .env File Example
```
AWS_PROFILE=ticketboat_ndelorme

ENVIRONMENT=dev
FIREBASE_AWS_SECRET_NAME=prod/firebase_credentials
FIREBASE_REALTIME_DATABASE_URL=https://ticket-boat-admin-default-rtdb.firebaseio.com/
AWS_DEFAULT_REGION=us-east-1
```

---

### Run the project

#### Pre-req:
Start and configure in .env a redis cache
```
docker run --name redis-cache --rm -p 6379:6379 redis:7
```

#### Option 1:
```
python src/app/main.py
```

#### Option 2:
* Navigate to the src/app/main.py file and click the Run/Debug button in VSCode on the top right

---

### Deploy to Staging
```
git checkout -b staging
git push --set-upstream origin staging
```

### Deploy to Production
```
git checkout -b production
git push --set-upstream origin production
```

### Un-Deploying
1. Before you can have Terraform un-deploy the project you must manually
   remove all the Images in the ECR repository. This is a safety mechanism
   to make sure you don't blow away images accidentally that you need.
2. Navigate to the Bitbucket project website
3. Click Pipelines in the left nav menu
4. Click Run pipeline button
5. Choose the branch you want to un-deploy
6. Choose the appropriate un-deploy Pipeline
   * un-deploy-staging
   * un-deploy-production
7. Click Run

### Run docker image locally
```
export AWS_PROFILE=ticketboat_ndelorme
aws ecr get-login-password \
      --region us-east-1 | \
      docker login \
        --username AWS \
        --password-stdin 317822790556.dkr.ecr.us-east-1.amazonaws.com/ticketboat-admin-api-staging_repository

docker run --env-file .env --rm -p 9000:8080 -it 317822790556.dkr.ecr.us-east-1.amazonaws.com/ticketboat-admin-api-staging_repository:latest 

# You will need to post an API Gateway json structure to test. Below is an example for /healthcheck
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

### Example API Gateway Json for /healthcheck
```
{
  "resource": "/{proxy+}",
  "path": "/healthcheck",
  "httpMethod": "GET",
  "headers": {
    "Content-Type": "application/json"
  },
  "multiValueHeaders": {
    "Content-Type": [
      "application/json"
    ]
  },
  "queryStringParameters": null,
  "multiValueQueryStringParameters": null,
  "pathParameters": null,
  "stageVariables": null,
  "requestContext": {
    "resourceId": "123456",
    "resourcePath": "/{proxy+}",
    "httpMethod": "GET",
    "extendedRequestId": "request-id",
    "requestTime": "02/Apr/2023:12:34:56 +0000",
    "path": "/healthcheck",
    "accountId": "123456789012",
    "protocol": "HTTP/1.1",
    "stage": "prod",
    "domainPrefix": "testPrefix",
    "requestTimeEpoch": 1586442782000,
    "requestId": "c1234567-1234-1234-1234-123456789012",
    "identity": {
      "cognitoIdentityPoolId": null,
      "accountId": null,
      "cognitoIdentityId": null,
      "caller": null,
      "sourceIp": "127.0.0.1",
      "principalOrgId": null,
      "accessKey": null,
      "cognitoAuthenticationType": null,
      "cognitoAuthenticationProvider": null,
      "userArn": null,
      "userAgent": "Custom User Agent String",
      "user": null
    },
    "domainName": "testPrefix.testDomainName",
    "apiId": "1234567890"
  },
  "body": null,
  "isBase64Encoded": false
}
```# ticketboat-test
