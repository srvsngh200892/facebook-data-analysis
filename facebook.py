import requests
import pymongo
import datetime
from collections import defaultdict
import json
import string
from stemming.porter2 import stem
import stopword
 
 
class follower_history:
 
    def follower_history_method(self,names_list,client,base_url,accesstoken,post_dict1,post_dict2):
 
        db=client.fb
 
        data=db.followers_history
 
        today = datetime.datetime.now()
        print today
 
        for user in names_list:

            #url= base_url + user
            
            #response = requests.get(url)
            
            #response2 = requests.get(url2)
            profile = post_dict2[user]
            
            profile2 = post_dict1[user]
            m = profile2["data"][0]["values"][0]["value"]
 
            dict1 = {'name':profile.get('name',0),'id':profile.get('id',0),'data_collected_on':str(today),'likes_by_countries':m,'company_overview':profile.get('company_overview',0),'description':profile.get('description',0),'about':profile.get('about',0),'founded':profile.get('founded',0),'likes':profile.get('likes',0),'mentions':profile.get('were_here_count',0),'talking_about':profile.get('talking_about_count',0)}
 
            data.update({'id':profile.get('data_collected_on',0)},dict1,upsert = True)
        print  "done"
 
 
 
 
class post_details:
 
 
     def post_details_methods(self,names_list,client,base_url,post_dict):
 
         db=client.fb
         data1=db.post_detls
         today = datetime.datetime.now()
 
         dict={}
 
         for user in names_list:
             
             data_dict=post_dict[user]
             array=data_dict["data"]

             for key  in array:
 
                 message_dict={}
                 message_dict['id']= key['id']
                 m=str(key['id'])
 
                 message_dict['from']= key['from']['id']
                 url2=base_url + m +'/likes/?summary=true'
                 response1 = requests.get(url2)
                 data_dict1 = json.loads(response1.content)
 
                 message_dict['total_likes']=data_dict1["summary"]["total_count"]
                 if key.get('comments'):
                    url2=base_url + m +'/comments/?summary=true'
                    response1 = requests.get(url2)
                    data_dict1 = json.loads(response1.content)
                    message_dict['total_comments']=data_dict1["summary"]["total_count"]
                 if key.get('message'):
                    message_dict['text']= key['message']
                 if key.get('picture'):
                    message_dict['picture']= key['picture']
                 if key.get('type'):
                    message_dict['type']= key['type']
                 if key.get('status_type'):
                    message_dict['status_type']= key['status_type']
                 if key.get('caption'):
                    message_dict['caption']= key['caption']
                 if key.get('description'):
                    message_dict['description']= key['description']
                 if key.get('name'):
                    message_dict['name']= key['name']
                 if key.get("shares"):
                    message_dict['share_count']= key['shares']['count']
                 message_dict['created_time']=key['created_time']
                 message_dict['data_collected_on_date']=str(today)
                 data1.update({'id':message_dict.get('id',0)},{'total_likes':message_dict.get('total_likes',0),'total_comments':message_dict.get('total_comments',0),'share_count':message_dict.get('share_count',0)},upsert=True)
 
class trending_words:
 
    def trends_methods(self,names_list,client,base_url,post_dict):
 
 
        db=client.fb
        t=db.trends
 
 
        for handle in names_list:
           
            data_dict=post_dict[handle]
            post=[]
            for i in range(len(data_dict["data"])):
                 array=data_dict["data"][i]
                 if array.get("message"):
                    post.append(data_dict["data"][i]["message"])
 
            def trend(ht, st):
                stopwords = stopword.get_data()
                d = defaultdict(int)
 
                mylist = []
                if ht=='n':
                   for i in range(len(post)):
                       m = post[i]
                       m = m.encode('ascii', 'ignore')
                       m = m.lower()
                       a = m.split()
                       for x in a:
                           if x[0] != '@' and x[0] != '#':
                              x =  x.translate(string.maketrans("",""), string.punctuation)
                              if x not in stopwords and x[:4] != 'http' and  x!='':
                                 if st == 'y':
                                    x = stem(x)
                                 mylist.append(x)
                else:
                   for i in range(len(post)):
                       m = post[i]
                       m = m.encode('ascii', 'ignore')
                       m = m.lower()
                       a = m.split()
                       for x in a:
                           if x[0] != '@':
                              if x[0] != '#':
                                 x =  x.translate(string.maketrans("",""), string.punctuation)
                              if x not in stopwords and x[:4] != 'http' and  x!='':
                                 if st == 'y':
                                    if x[0] != '#':
                                       x = stem(x)
                                 mylist.append(x)
                for i in mylist:
                    d[i] += 1
                result = json.dumps(d)
 
 
                mydict = {'id' : handle, 'date_n_time' : datetime.datetime.now(), 'ht' : ht, 'st' : st, 'trending_terms' : result}
                t.insert(mydict)
 
 
 
        trend( 'y', 'y')
        trend( 'y', 'n')
        trend( 'n', 'y')
        trend( 'n', 'n')
 
class brand_tagging:
 
 
    def brand_tagging_methods(self,names_list,client,base_url,post_dict):
 
        db=client.fb
 
        d=db.brands_taging
 
        today = datetime.datetime.now()
 
 
 
 
 
 
        noise=["and","today","http","com","or","be","now","india","books","enjoy","emi","accents","w","worth","idea","royal","wow","black","sliver","music","id","answer","trust","up","colors","UP","online","answers","no","jax","today","reach","sure","a","one","too.","look", "X","about","for", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount", "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as", "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the", "flipkart", 'myntra', 'jabong', 'homeshop18', 'youve']
 
        special_char=["@",",","#",".",";",":","!","##","/","\\","?","??"]
 
        baseurl='http://180.151.97.90/brand_tagging/getbrand?title='
 
 
        s1=[]
 
 
        s2=[]
 
 
        s3=[]
 
 
 
        post=[]
        for handle in names_list:
            data_dict=post_dict[handle]
            post=[]
            for i in range(len(data_dict["data"])):
                array=data_dict["data"][i]
                if array.get("message"):
                   post.append(data_dict["data"][i]["message"])
 
            for i in range(len(post)):
                m=post[i]
                for a in special_char:
                    if a in m:
                       m=m.replace(a,"")
                s3.append(m)
 
            for i in range(len(s3)):
                lower=s3[i].lower();
                new_s=lower.split()
                data=[ a for a in new_s if a not in noise ]
                s4= ' '.join(data)
                s2.append(s4)
 
            brand_frequency={}
            brand={}
            lst=[]
            for s in s2:
 
                url = baseurl + s +'&country=social'
                response = requests.get(url)
 
                a=response.json()
                if a['brand']!="":
                   if a['brand'] not in brand_frequency.keys():
                      brand_frequency[a['brand']]=1
                   else:
                      brand_frequency[a['brand']]+=1
 
            lst.append(brand_frequency)
            brand['brands']=lst
 
            #brand_frequency=sorted(brand_frequency.iteritems(), key=lambda (k,v):(v,k), reverse=True)
            brand["ac_name"]=handle
            brand['data_collected_on']=str(today)
 
            d.update({'ac_name':brand.get('ac_name',0)},brand,upsert=True)
 
            del s3[0:len(s3)]
            del s2[0:len(s2)]
            del post[0:len(post)]
 
 
class comment_detail:
 
 
    def comment_details_method(self,names_list,client,base_url,post_dict):
 
        #client=pymongo.MongoClient()
        db=client.fb
        data1=db.commentss_detailss
 
        today = datetime.datetime.now()
 
        for user in names_list:
           
            data_dict=post_dict[user]
 
            array=data_dict['data']
 
            print type(array)
 
            for key  in array:
 
 
                if key.get('comments'):
                   m= key['comments']['data']
 
                   for k in m:
 
                       message_dict={}
                       message_dict['post_id']=key['id']
                       message_dict['ac_id']= key['from']['id']
                       message_dict['user_id']=k['from']['id']
                       message_dict['comment_id']=k['id']
                       message_dict['comment_text']=k['message']
                       message_dict['like_count']=k['like_count']
                       message_dict['user_likes']=k['user_likes']
                       message_dict['create_time']=k['created_time']
                       message_dict['data_collected_on_date']=str(today)
 
                       data1.update({'comment_id':message_dict.get('comment_id',0)},message_dict,upsert=True)
 
 
 
class user_details:
 
    def user_details_method(self,names_list,client,base_url,post_dict):
 
        client=pymongo.MongoClient()
        db=client.fb
        data=db.liking
        data1=db.user_details
 
 
 
        for user in names_list:
            
            data_dict=post_dict[user]
            user_id=[]
            m=[]
            z={}
            array=data_dict["data"]
            for a in array:
                if a.get("likes"):
                    m=a["likes"]["data"]
                for x in m:
                    user_id.append(x['id'])
            for i  in range(len(user_id)):
                b=user_id[i]
                k=list(data.find({"like_id":b},{"_id":0,"ac_id":0,"like_id":0,"dummy":0}))
                z['ac_id']=a['from']['id']
                z["user_id"]=b
                z["user_like_details"]=k
                data1.update({'user_id':z.get('user_id',0)},z,upsert=True)
 
class post_likes_comments:
 
    def post_like_method(self,names_list,client,base_url,post_dict):
 
        db=client.fb
        data1=db.post_detls
        data2=db.post_likes_comments_details
 
        today = datetime.datetime.now()
 
 
        name_list = [
            '102988293558','6466648220','245106162209654'
        ]
 
 
 
        #base_url = 'https://graph.facebook.com/'
 
 
        dict={}
        for user in names_list:
            
            data_dict = post_dict[user]
            
            
            array=data_dict["data"]
 
        for name in name_list:
            lis=[]
            message_dict={}
            lis=list(data1.find({'from':name},{'id':1,'_id':0}))
 
            for a in lis:
 
                url2=base_url + a['id'] +'/likes/?summary=true'
                response1 = requests.get(url2)
                data_dict1 = json.loads(response1.content)
 
                message_dict['total_likes']=data_dict1["summary"]["total_count"]
                message_dict['text_id']=a['id']
                message_dict['act_id']=name
 
 
                #message_dict['share_count']= a['share_count']
 
 
 
                url3=base_url + a['id'] +'/comments/?summary=true'
                response2 = requests.get(url3)
                data_dict2 = json.loads(response2.content)
                #print data_dict2
                if data_dict2.get('summary'):
                   message_dict['total_comments']=data_dict2["summary"]["total_count"]
 
 
                #print message_dict
                data2.update({'text_id':message_dict.get('text_id',0)},message_dict,upsert=True)
        print "complt"
 
 
 
 
if __name__ == '__main__':
 
    base_url = 'https://graph.facebook.com/'
 
    client = pymongo.MongoClient()
 
    accesstoken = "1408129306113261|18650670cff9fb8022dde4b98fc84d3a"
 
    post_dict = {}
    post_dict1 = {}
    post_dict2 = {}
    
 
    names_list = [
            'Flipkart','myjabong','Snapdeal','myntra'
        ]
 
    for user in names_list:
        url = base_url + user + '/posts?access_token=1408129306113261|18650670cff9fb8022dde4b98fc84d3a'
        response = requests.get(url)
        profile = response.json()
        post_dict[user] = profile
        url2 = base_url + user + "/insights/page_fans_country?access_token=1408129306113261|18650670cff9fb8022dde4b98fc84d3a"
        response2 = requests.get(url2)
        profile2 = response2.json()
        post_dict1[user]=profile2
        url3= base_url + user
        response3 = requests.get(url3)
        profile3=response3.json()
        post_dict2[user]=profile3
        print user,"complt"
 
    fol_hist = follower_history()
    fol_hist.follower_history_method(names_list, client, base_url, accesstoken,post_dict1,post_dict2)
 
    post_hist = post_details()
    post_hist.post_details_methods(names_list, client, base_url, post_dict)
 
    trendss = trending_words()
    trendss.trends_methods(names_list, client, base_url, post_dict)
 
    brands = brand_tagging()
    brands.brand_tagging_methods(names_list, client, base_url, post_dict)
 
    comment = comment_detail()
    comment.comment_details_method(names_list, client, base_url, post_dict)
 
    user = user_details()
    user.user_details_method(names_list, client,base_url, post_dict)
 
    details = post_likes_comments()
    details.post_like_method(names_list, client,base_url,post_dict)
