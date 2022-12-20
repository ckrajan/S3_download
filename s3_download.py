import boto3
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
REGION = os.environ.get("REGION")

clips_bucket = os.environ.get("clips_bucket")

s3_client = boto3.client(
    's3',
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

s3_resource = boto3.resource(
    's3',
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

my_bucket = s3_resource.Bucket(clips_bucket)

download_path = "/home/chathushkavi/Downloads/"

#Get S3 Signed URLs
def get_signed_s3_urls():
    response = create_presigned_url("input/")
    # print(response)
    return response

def create_presigned_url(object):
    try:
        key = object
        url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': clips_bucket, 'Key': key, },
                ExpiresIn=600000,
        )

        res = s3_client.list_objects(
                Bucket=clips_bucket,
                MaxKeys=50
            )

        result_set = []

        if "Contents" in res:
            for result in res["Contents"]:
                if result["Size"] > 0:
                    result_set.append(result)
        
        else:
            print("file not found")

        # for object_summary in my_bucket.objects.filter(Prefix="input/"):
        #     print(object_summary.key)

        if not os.path.exists(download_path + 'Downloaded_clips'):
            os.makedirs(download_path + 'Downloaded_clips/', exist_ok=True)

        for i in range(len(result_set)):
            file_name = result_set[i]["Key"]
            if file_name.startswith("input/"):

                print(f"downloading {file_name}")
                
                if not os.path.exists(download_path + 'Downloaded_clips/'+ file_name.rsplit('/', 1)[-2]):
                    os.makedirs(download_path + 'Downloaded_clips/'+ file_name.rsplit('/', 1)[-2], exist_ok=True)

                s3_client.download_file(
                    clips_bucket,
                    file_name,
                    download_path + 'Downloaded_clips/'+ file_name.rsplit('/', 1)[-2] + '/' + file_name.rsplit('/', 1)[-1],
                )


        # s3_client.download_file(clips_bucket, '1.mp4', '/home/chathushkavi/Downloads/clip.mp4')
                    
    except boto3.ClientError as e:
        print(e)
        return None

    # return url      



get_signed_s3_urls()