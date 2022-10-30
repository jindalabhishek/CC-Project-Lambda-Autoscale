from boto3 import client as boto3_client
from boto3.dynamodb.conditions import Key, Attr
import face_recognition
import pickle
import os
import boto3

input_bucket = 'input-videos-cc'
output_bucket = 'output-csv-cc'

table_name = 'student_data'
# db_client = boto3_client('dynamodb')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

bucket_client = boto3_client('s3')
# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	print(data)
	return data

def download_and_save_video(event):
	if event and event['Records'] and len(event['Records']) > 0 and event['Records'][0]['s3'] and event['Records'][0]['s3']['object'] and event['Records'][0]['s3']['object']['key']:
		video_name = event['Records'][0]['s3']['object']['key']
		bucket_client.download_file(input_bucket, video_name, '/tmp/'+video_name)
		return video_name
	return None


def query_data_save_to_csv(video_name, object_name):
	response = table.scan(FilterExpression=Attr('name').eq(object_name))

	print(response['Items'])

	db_data = response['Items'][0]

	row_data = ','.join([db_data['name'], db_data['major'], db_data['year']]) + '\n'
	output_file_path = '/tmp/'
	output_file_name = 'output-file-' + video_name.replace('.mp4', '') + '.csv' 
	output_file_qualified_name = output_file_path + output_file_name
	
	output_file = open(output_file_qualified_name, 'a')
	output_file.write(row_data)
	output_file.close()

	bucket_client.upload_file(output_file_qualified_name, output_bucket, output_file_name)


def face_recognition_handler(event, context):
	print('Hello')
	# print("Received event: " + json.dumps(event, indent=2))
	video_name = download_and_save_video(event);
	print(video_name)
	video_file_path = '/tmp/' + video_name
	frames_path = '/tmp/'
	os.system("ffmpeg -i " + str(video_file_path) + " -r 1 " + str(frames_path) + "image-%3d.jpeg")
	images = os.listdir(frames_path)
	print(images)
	known_image_data = open_encoding('/home/app/encoding')
	# for image in images:
	unknown_image = face_recognition.load_image_file(frames_path+'image-001.jpeg')
	unknown_encoding = face_recognition.face_encodings(unknown_image)[0]
	# print(unknown_encoding)
	results = face_recognition.compare_faces(known_image_data['encoding'], unknown_encoding)
	# print(results)

	true_index = 0
	for index in range(0, len(results)):
		if results[index]:
			true_index = index
			print(true_index)
			break

	print(known_image_data['name'][true_index])
	object_name = known_image_data['name'][true_index]
	
	query_data_save_to_csv(video_name, object_name)

	
# face_recognition_handler()
