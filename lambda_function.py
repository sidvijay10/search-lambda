#TEST
import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
import urllib.request
import requests
from requests_aws4auth import AWS4Auth

from botocore.exceptions import ClientError

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')


REGION = 'us-east-1'
HOST = 'search-photos-df2awflxl7ypfve2bic7gqst6y.us-east-1.es.amazonaws.com'
INDEX = 'photos'
url = "https://search-photos-df2awflxl7ypfve2bic7gqst6y.us-east-1.es.amazonaws.com/photos/_doc"

def lambda_handler(event, context):
    # retrieve message from API gateway
    print(event)
    msg_from_user = event['messages'][0] # CHANGE TO CORRECT FORMATTING
    
    # Initiate conversation with Lex
    response = client.recognize_text(
            # note: botId and botAliasId 
            botId='1ZSHG26S3S', 
            botAliasId='NLHGEVUTTZ', 
            localeId='en_US',
            sessionId='testuser',
            # CHANGE TO CORRECT FORMATTING
            text=event['messages'][0])
            #text=msg_from_user['unstructured']['text'])
    
     # from response -- need to extract the keywords
    query_terms = [] 
    query_1 = response["interpretations"][0]["intent"]["slots"]["slot1"]["value"]["resolvedValues"][0]
    query_terms.append(word_stem(query_1))
    
    if not response["interpretations"][0]["intent"]["slots"]["slot2"] == None:
        query_2 = response["interpretations"][0]["intent"]["slots"]["slot2"]["value"]["resolvedValues"][0]
        query_terms.append(word_stem(query_2))
    
    
    print(query_terms)
    
    # search the keywords in OpenSearch
    elastic_query_results1 = query(query_terms[0])
    
    if len(query_terms) > 1:
        elastic_query_results2 = query(query_terms[1])
        original_list = elastic_query_results1 + elastic_query_results2
        query_results = list(set((a, tuple(b)) for a, b in original_list))
    
    else: 
        elastic_query_results1
        query_results = list(set((a, tuple(b)) for a, b in elastic_query_results1))
    
    print(query_results)
    
    
    message_resp = []
    if len(query_results) > 0:
        # Construct list of photo objects
        photos = []
        for result in query_results:
            photo = {
                'url': result[0],
                'labels': result[1]
            }
            photos.append(photo)
        
        # Construct response message
        resp_message = {
            'type': 'structured',
            'structured': {
                'SearchResponse': {
                    'results': photos
                }
            }
        }
        message_resp.append(resp_message)
    else:
        # Construct response message for no results found
        resp_message = {
            'type': 'unstructured',
            'unstructured': {
                'text': 'No results found for search query'
            }
        }
        message_resp.append(resp_message)
    
    # Construct final response dictionary
    resp = {
        'statusCode': 200,
        'headers': {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*',
        },
        'messages': message_resp
    }
    return resp
    

def query(term):
    q = {
        'size': 50,
        'query': {
            'bool': {
                'must': [
                    {
                        'match': {
                            'labels': term
                        }
                    }
                ]
            }
        }
    }
    client = OpenSearch(
        hosts=[{            'host': HOST,            'port': 443        }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    res = client.search(index=INDEX, body=q)
    hits = res['hits']['hits']
    results = []
    for hit in hits:
        result = (hit['_source']['objectKey'], hit['_source']['labels'])
        results.append(result)
    return results


def word_stem(word):
    if word.endswith("s"):
        return word[:-1]
    else:
        return word

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)


