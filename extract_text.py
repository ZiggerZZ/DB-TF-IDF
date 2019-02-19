import json
import glob
docs = glob.glob("texts/*.json")
for doc in docs:
    json_file = open(doc)
    json_str = json_file.read()
    json_data = json.loads(json_str)
    text = json_data['text']
    f = open(doc[:-4]+'txt','w')
    f.write(text)
    f.close()