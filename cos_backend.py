import ibm_boto3
import ibm_botocore

class COSBackend:

    def __init__(self):
        service_endpoint = 'https://s3.eu-de.cloud-object-storage.appdomain.cloud'
        secret_key = '648f22bd139ff9133c35027201dadd159f10a7d1dbb2cd51'
        access_key = '3c89d1b7e5364c4b92587ddc0a932c28'
        client_config = ibm_botocore.client.Config(max_pool_connections=200, user_agent_extra='pywren_ibm_cloud')
        self.cos_client = ibm_boto3.client('s3',
                                            aws_access_key_id=access_key,
                                            aws_secret_access_key=secret_key,
                                            config=client_config,
                                            endpoint_url=service_endpoint)

    def put_object(self, bucket_name, key, data):

        try: 
            res = self.cos_client.put_object(Bucket=bucket_name, Key=key, Body=data)
            status = 'OK' if res['ResponseMetadata']['HTTPStatusCode'] == 200 else 'Error'
            try:
                print('PUT Object{} - Sixe: {} - {}'.format(key, sizeof_fmt(len(data)), status))
            except:
                 print('PUT Object {} {}'.format(key, status))
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def get_object (self, bucket_name, key, stream=False, extra_get_args={}):
        
        try: 
            r= self.cos_client.get_object(Bucket=bucket_name, Key=key, **extra_get_args)
            if stream:
                data= r['Body']
            else:
                data = r['Body'].read()
            return data
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def head_object(self, bucket_name, key):

        try: 
            metadata = self.cos_client.head_object(Bucket=bucket_name, Key=key)
            return metadata['ResponseMeta']['HTTPHeaders']
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def delete_object(self, bucket_name, key):
        
            return self.cos_client.delete_object(Bucket=bucket_name, Key=key)