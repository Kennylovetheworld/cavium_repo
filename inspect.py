import bz2
import json

file_path = "RC_2016-12.bz2"
bz_file = bz2.BZ2File(file_path)
data = bz_file.readline()
data = json.loads(data.decode('utf-8'))
print(data['subreddit_id'])
