import xml.etree.cElementTree as ET
from pymongo import MongoClient
from bs4 import BeautifulSoup

file_name = r'Posts.xml'
MIN_CODE_LEN = 40


def remove_no_code():
    mongo_collection = get_collection()
    print mongo_collection.count()
    posts = mongo_collection.find()
    for post in posts:
        if 'codes' not in post.keys() or len(post['codes']) < 2:
            mongo_collection.remove({'Id': post['Id']})
    print mongo_collection.count()


def build_orig_db():
    mongo_collection = get_collection()
    mongo_collection.drop()
    mongo_collection.ensure_index("Id", unique=True)

    questions_cnt = 0
    answers_cnt = 0

    for event, elem in ET.iterparse(file_name, events=("start", "end")):
        if event == 'start':
            post_type = elem.get('PostTypeId')
            if post_type == '1':
                title = elem.get('Title')
                tags = elem.get('Tags')
                post_id = elem.get('Id')
                if mongo_collection.find({"Id": post_id}).count() == 0 and "<java>" in tags.lower():
                    rep_id = mongo_collection.insert({"Id": post_id, "Tags": tags, "Title": title}, w=0)
                    questions_cnt += 1
            if post_type == '2':
                pass
            elem.clear()

    print 'inserted:' + str(questions_cnt)

    for event, elem in ET.iterparse(file_name, events=("start", "end")):
        if event == 'start':
            post_type = elem.get('PostTypeId')
            if post_type == '2':
                parentId = elem.get('ParentId')
                parent = mongo_collection.find_one( { "Id" : parentId })
                if parent is None:
                    continue
                soup = BeautifulSoup(elem.attrib['Body'])
                c = soup.findAll('code')
                if len(c) == 1 and int(elem.attrib['Score']) > 2:
                    code = c[0].prettify(formatter=None)
                    if len(code) > MIN_CODE_LEN:
                        code = code.replace("<code>", "").replace("</code>", "")
                        mongo_collection.update( { "Id" : parentId}, { "$addToSet": { "codes" : code} })
                        answers_cnt += 1
            elem.clear()

    print 'inserted answers:' + str(answers_cnt)


def get_collection():
    try:
        client = MongoClient()
    except:
        exit()
    mongo_db = client.sof
    mongo_collection = mongo_db.posts
    return mongo_collection


if __name__ == "__main__":
    build_orig_db()
    #remove_no_code()

