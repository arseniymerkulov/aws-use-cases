import time
import logging
import os


from aws_manager import AWSUser, AWSIAMManager, AWSS3Manager
logging.basicConfig(level=logging.INFO)


user = AWSUser('user', 'user@gmail.com')
iam = AWSIAMManager(user)
iam.create_user()
time.sleep(10)

s3 = AWSS3Manager(user, 'eu-west-1')
s3.create_bucket()
print(s3.get_buckets())

s3.upload_file('data/image.jpg')
print([obj.key for obj in s3.get_bucket().objects.all()])

download_path = 'data/downloaded_image.jpg'
s3.download_file(download_path, 'image.jpg')
if os.path.exists(download_path):
    os.remove(download_path)
    print('downloaded file removed')

s3.delete_bucket()
iam.delete_user()
