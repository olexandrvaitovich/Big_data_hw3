import pyspark
from pyspark.sql import functions as F
from time import time
import json

sc = pyspark.SparkContext('spark://ec2-35-174-242-101.compute-1.amazonaws.com:7077')
spark = pyspark.sql.SparkSession.builder.master("spark://ec2-35-174-242-101.compute-1.amazonaws.com:7077").enableHiveSupport().getOrCreate()

def first(data):
    top10 = data.groupby(data._c0).count()
    top10 = top10.orderBy('count', ascending=False).limit(10)
    top10_ids = top10.select('_c0').collect()
    top10_counts = top10.select('count').collect()

    videos = []

    for i in range(10):
        entrances = data.filter(data._c0==top10_ids[i]._c0).orderBy('_c1', ascending=False)
        video = {}
        trending_days = [{'date':None, 'views':0, 'likes':0, 'dislikes':0} for j in range(top10_counts[i]['count'])]
        entrances_count = 0
        for e in entrances.collect():
            trending_days[entrances_count]['date'] = e._c1
            trending_days[entrances_count]['views'] = e._c7
            trending_days[entrances_count]['likes'] = e._c8
            trending_days[entrances_count]['dislikes'] = e._c9
            if entrances_count==0:
                video['id'] = e._c0
                video['title'] = e._c2
                video['description'] = e._c15
                video['latest_views'] = e._c7
                video['latest_likes'] = e._c8
                video['latest_dislikes'] = e._c9
            entrances_count+=1
        video['trending_days'] = trending_days
        videos.append(video)
    
    return {'videos':videos} 


import datetime
def second(data):

    data = data.withColumn('_c1', F.to_date(F.unix_timestamp(F.col('_c1'), 'yy.dd.MM').cast('timestamp')))

    data = data.orderBy('_c1')
    beg = data.limit(2).collect()[1]._c1
    end = beg+datetime.timedelta(days=7)
    
    weeks = []
    week_data = data.filter(F.col('_c1').between(beg, end))

    while not week_data.rdd.isEmpty():
        
        week = {}
        week['start_date'] = str(beg)
        week['end_date'] = str(end)
        categories = {}
        for sample in week_data.collect():
            if sample._c4 in categories:
                if sample._c0 in categories[sample._c4]:
                    categories[sample._c4][sample._c0].append(sample._c7)
                else:
                    categories[sample._c4][sample._c0] = [sample._c7]
            else:
                categories[sample._c4] = {sample._c0:[sample._c7]}
        for k in categories.keys():
            categories[k] = dict(filter(lambda x: len(x[1])>1, categories[k].items()))
        
        summaries = {k:[len(categories[k]), list(categories[k].keys()), sum(list(map(lambda x: int(max(x[1]))-int(min(x[1])), categories[k].items())))] for k in categories.keys()}
        
        top_category = max(summaries.items(), key=lambda x: x[1][2])
        week['category_id'] = top_category[0]
        week['number_of_videos'] = top_category[1][0]
        week['total_views'] = top_category[1][2]
        week['videos_ids'] = top_category[1][1]
        
        weeks.append(week)
        
        beg = end+datetime.timedelta(days=1)
        end = beg+datetime.timedelta(days=7)
        week_data = data.filter(F.col('_c1').between(beg, end))

    return {'weeks':weeks}



def third(data):
    data = data.withColumn('_c1', F.to_date(F.unix_timestamp(F.col('_c1'), 'yy.dd.MM').cast('timestamp')))
    data = data.orderBy('_c1')
    beg = data.limit(2).collect()[1]._c1
    end = beg+datetime.timedelta(days=30)
    months = []
    month_data = data.filter(F.col('_c1').between(beg, end))
    used_ids = set()
    
    while not month_data.rdd.isEmpty():
        month = {'start_date':str(beg), 'end_date':str(end)}
        tags = {}
    
        for e in month_data.collect():
            if e._c0 in used_ids:
                continue

            sample_tags = e._c6
            sample_tags = sample_tags.split('|')
            used_ids.add(e._c0)
            for j in sample_tags:
                if j in tags:
                    tags[j].append(e._c0)
                else:
                    tags[j] = [e._c0]
    
        top_tags = sorted(tags.items(), key=lambda x:len(x[1]))[-10:]
        top_tags = list(map(lambda x: {'tag':x[0], 'number_of_videos':len(x[1]), 'videos_ids':x[1]}, top_tags))
        month['tags'] = top_tags
        months.append(month)
        beg = end+datetime.timedelta(days=1)
        end = beg+datetime.timedelta(days=30)
        month_data = data.filter(F.col('_c1').between(beg, end))
    return {'months': months}

def forth(data):
    data = data.withColumn('_c1', F.to_date(F.unix_timestamp(F.col('_c1'), 'yy.dd.MM').cast('timestamp')))
    data = data.orderBy('_c1', ascending=False)
    
    channels = {}

    for i in data.collect():
        if i._c3 not in channels:
            if not i._c7.isdigit():
                continue
            channels[i._c3] = {'channel_name':i._c3, 'total_views':int(i._c7), 'start_date':i._c1, 'end_date':i._c1, 'videos_views':[{'video_id':i._c0, 'views':i._c7}], 'videos':set([i._c0])}
        else:
            if channels[i._c3]['start_date'] > i._c1:
                channels[i._c3]['start_date'] = i._c1
            if i._c0 not in channels[i._c3]['videos']:
                channels[i._c3]['videos'].add(i._c0)
                channels[i._c3]['videos_views'].append({'video_id':i._c0, 'views':i._c7})
                channels[i._c3]['total_views'] += int(i._c7)
    
    top20 = sorted(channels.items(), key=lambda x: x[1]['total_views'])[-20:]
    for i in range(len(top20)):
        del top20[i][1]['videos']
        top20[i][1]['start_date'] = str(top20[i][1]['start_date'])
        top20[i][1]['end_date'] = str(top20[i][1]['end_date'] )
    return {'channels':list(map(lambda x: {x[0]:x[1]}, top20))}

def fifth(data):
    
    channels = {}

    for i in data.collect():
        if i._c3 not in channels:
            if not i._c7.isdigit():
                continue
            channels[i._c3] = {'channel_name':i._c3, 'total_trending_days':1, 'videos_days':{i._c0:{'video_id':i._c0, 'video_title':i._c2, 'trending_days':1}}}
        else:
            channels[i._c3]['total_trending_days']+=1
            if i._c0 in channels[i._c3]['videos_days']:
                channels[i._c3]['videos_days'][i._c0]['trending_days']+=1
            else:
                channels[i._c3]['videos_days'][i._c0] = {'video_id':i._c0, 'video_title':i._c2, 'trending_days':1}
    top10 = sorted(channels.items(), key=lambda x: x[1]['total_trending_days'])[-10:]
    for i in range(len(top10)):
        top10[i][1]['videos_days'] = list(top10[i][1]['videos_days'].values())
    return {'channels':list(map(lambda x: {x[0]:x[1]}, top10))}


def sixth(data):

    categories = {}

    for i in data.collect():
        if i._c4 not in categories:
            if not i._c7.isdigit():
                continue
            categories[i._c4] = {'category_id': i._c4, 'videos':{i._c0:{'video_id':i._c0, 'video_title': i._c2, 'ratio_likes_dislikes':int(i._c8)/int(i._c9) if i._c9!='0' else float('inf'), 'Views':i._c7}}}
            if int(i._c7)<100000:
                del categories[i._c4]['videos'][i._c0]
        else:
            if i._c0 not in categories[i._c4]['videos'] and int(i._c7) >= 100000:
                categories[i._c4]['videos'][i._c0] = {'video_id':i._c0, 'video_title': i._c2, 'ratio_likes_dislikes':int(i._c8)/int(i._c9) if i._c9!='0' else float('inf'), 'Views':i._c7}
            elif int(i._c7) >= 100000 and (int(i._c8)/int(i._c9) if i._c9!='0' else float('inf'))>categories[i._c4]['videos'][i._c0]['ratio_likes_dislikes']:
                categories[i._c4]['videos'][i._c0]['ratio_likes_dislikes'] = int(i._c8)/int(i._c9) if i._c9!='0' else float('inf')
                categories[i._c4]['videos'][i._c0]['Views'] = i._c7
    result_categories = {}
      
    all_videos = {}
    for k in categories.keys():
        all_videos = {**all_videos, **categories[k]['videos']}
    
    top10_videos = sorted(all_videos.items(), key=lambda x:x[1]['ratio_likes_dislikes'])[-10:]
    
    for i in top10_videos:
            
        for k in categories.keys():
            
            if i[0] in categories[k]['videos']:
            
                if k in result_categories:
                    result_categories[k]['videos'].append(i[1])
                else:
                    result_categories[k] = {**categories[k]}
                    result_categories[k]['videos'] = [i[1]]
                break

    return {'categories':list(result_categories.values())}

import subprocess
import sys
if __name__=='__main__':
    bucketname = sys.argv[1]
    result_dict = {}
    result_dict['cluster_params'] = {'ram':8, 'cpus':2, 'nodes':3}
    result_dict['execution_params'] = {'executors':4, 'ram_per_executor':6.5, 'cpus_per_executors':2}
    files = subprocess.check_output(['aws', 's3', 'ls', bucketname]).decode('utf-8').split('\n')[:-1]
    files = list(filter(lambda x: x.endswith('.csv'), list(map(lambda x:x.split()[-1], files))))
    for f in files:
        subprocess.call(['aws', 's3', 'cp', 's3://{}/{}'.format(bucketname, f), '.'])
    files = subprocess.check_output(['ls']).decode('utf-8').split('\n')[:-1]
    files = list(filter(lambda x: x.endswith('.csv'), files))
    names = ['first', 'second', 'third', 'forth', 'fifth', 'sixth']
    funcs = [first, second, third, forth, fifth, sixth] 
    for f in files:
        data = spark.read.csv(f)
        result_dict[f] = {'size':data.count()}
        for i in range(len(names)):
            start = time()
            temp_res = funcs[i](data)
            res_time = time()-start
            result_dict[f]['time_{}'.format(names[i])] = res_time
            
            result_dict[f]['size_of_result_{}'.format(names[i])] = len(temp_res[list(temp_res.keys())[0]])
            with open('{}_{}.json'.format(names[i], f.split('.')[0]), 'w+') as jf:
                json.dump(temp_res, jf)

    files = subprocess.check_output(['ls']).decode('utf-8').split('\n')[:-1]
    files = list(filter(lambda x: x.endswith('.json'), files))
    for i in range(len(files)):
        subprocess.call(['aws', 's3', 'cp', files[i], 's3://{}/copycat_inc/{}/{}/result.json'.format(bucketname, (i%6)+1), files[i].split('_')[1]])
    with open('All_results.json', 'w+') as f:
        json.dump(result_dict, f)
    subprocess.call(['aws', 's3', 'cp', 'All_results.json', 's3://All_results.json'])
    subprocess.call(['rm', '*.json'])
    subprocess.call(['rm', '*.csv'])
