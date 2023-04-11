from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
import logging
import boto3
import os


logger = logging.getLogger(__name__)


class AWSUser:
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email
        self._access_key_id = ''
        self._access_key_secret = ''

    @property
    def access_key(self):
        return self._access_key_id, self._access_key_secret

    @access_key.setter
    def access_key(self, access_key):
        self._access_key_id = access_key[0]
        self._access_key_secret = access_key[1]


class AWSIAMManager:
    def __init__(self, user: AWSUser):
        self.user = user
        self.group = 'regular-permissions'
        # access for IAM operations
        self.client = boto3.Session(profile_name='user-manager').client('iam')

    def create_user(self):
        try:
            response = self.client.create_user(
                UserName=self.user.name,
            )
            self.client.get_waiter('user_exists').wait(UserName=self.user.name)
            logger.info(f'user {self.user.name} created')

            response = self.client.add_user_to_group(
                GroupName=self.group,
                UserName=self.user.name
            )
            logger.info(f'user {self.user.name} added to group {self.group}')

            response = self.client.create_access_key(
                UserName=self.user.name
            )
            logger.info(f'access key created for user {self.user.name}')

            access_key_id = response['AccessKey']['AccessKeyId']
            access_key_secret = response['AccessKey']['SecretAccessKey']
            self.user.access_key = (access_key_id, access_key_secret)

        except ClientError as e:
            logger.error(e)
            return False
        return True

    def delete_user(self):
        try:
            response = self.client.remove_user_from_group(
                GroupName=self.group,
                UserName=self.user.name
            )
            logger.info(f'user {self.user.name} removed from group')

            key_id, _ = self.user.access_key
            response = self.client.delete_access_key(
                AccessKeyId=key_id,
                UserName=self.user.name,
            )
            logger.info(f'access key removed from user {self.user.name}')

            response = self.client.delete_user(UserName=self.user.name)
            logger.info(f'user {self.user.name} deleted')
        except ClientError as e:
            logger.error(e)
            return False
        return True


class AWSS3Manager:
    def __init__(self, user: AWSUser, region: str):
        # capitalization isn't allowed in bucket names
        self.bucket = f'aibb-aws-{user.name.lower()}'
        self.region = region
        self.user = user

        key_id, key_secret = user.access_key
        session = boto3.Session(aws_access_key_id=key_id,
                                aws_secret_access_key=key_secret)
        self.client = session.client('s3', region_name=region)
        self.resource = session.resource('s3', region_name=region)

    def create_bucket(self):
        try:
            location = {'LocationConstraint': self.region}
            self.client.create_bucket(Bucket=self.bucket, CreateBucketConfiguration=location)
            logger.info(f'bucket {self.bucket} created for {self.region} region')
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def delete_bucket(self):
        try:
            bucket = self.resource.Bucket(self.bucket)
            bucket.objects.all().delete()
            logger.info(f'empty bucket {self.bucket}')

            response = self.client.delete_bucket(Bucket=self.bucket)
            logger.info(f'bucket {self.bucket} deleted')
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def upload_file(self, file_name, object_name=None):
        if object_name is None:
            object_name = os.path.basename(file_name)

        try:
            response = self.client.upload_file(file_name, self.bucket, object_name)
            logger.info(f'file {object_name} uploaded to bucket {self.bucket}')
        except S3UploadFailedError as e:
            logger.error(e)
            return False
        return True

    def download_file(self, file_name, object_name):
        try:
            self.client.download_file(self.bucket, object_name, file_name)
            logger.info(f'file {object_name} downloaded from bucket {self.bucket}')
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def get_bucket(self):
        try:
            return self.resource.Bucket(self.bucket)
        except ClientError as e:
            logger.error(e)
            return None

    def get_buckets(self):
        try:
            response = self.client.list_buckets()
            assert 'Buckets' in response
            return response['Buckets']
        except (AssertionError, ClientError) as e:
            logger.error(e)
            return None
