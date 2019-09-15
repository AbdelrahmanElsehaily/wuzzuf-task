import json
from flask import Flask, url_for, Response, jsonify
from flask_cors import CORS
from ibm_watson import ToneAnalyzerV3
import pandas as pd
from elasticsearch import Elasticsearch
app = Flask(__name__)
app.config['TESTING'] = True
CORS(app)
@app.route("/")
@app.route("/hotel-tone/<hotel_name>")
def get_hotel_tone(hotel_name):
    '''
    return normalized score using hotel name
    '''
    text = hotels_df.loc[[hotel_name]]['reviews.text'].values.tolist()

    tone_analyzer = ToneAnalyzerV3(
        version='2017-09-21',
        iam_apikey="2wWjeivhvC9z64A2sSTcJxmq2C6c-6MspUeRGzq9J_Qm",
        url="https://gateway-lon.watsonplatform.net/tone-analyzer/api"
    )

    scores = {}
    for x in text:
        tone_analysis = tone_analyzer.tone(
            {'text': x},
            content_type='application/json'
        ).get_result()
        for tone_item in tone_analysis['document_tone']['tones']:
            try:
                scores[tone_item['tone_name']]['score']+= tone_item['score']
                scores[tone_item['tone_name']]['count']+= 1
            except:
                scores[tone_item['tone_name']] = {'score':tone_item['score'], 'count':1}
    print('finished calculating scores')
    normalized_scores = dict([(k, scores[k]['score']/scores[k]['count']) for k in scores])
    return jsonify(normalized_scores)


@app.route("/hotel-indexer")
def elastic_indexer():
    es = Elasticsearch()
    # es.indices.delete(index='test-index', ignore=[400, 404])
    reviews_columns = [x for x in hotels_df.columns if x.startswith('reviews')]
    for name in hotels_df['name'].unique():
        reviews_data = []
        hotel_data = hotels_df.loc[[name]]
        hotel_dict = dict(hotel_data[[x for x in hotels_df.columns if x not in reviews_columns]].iloc[0])
        for _, row in hotel_data.iterrows():
            reviews_data.append(dict(row[reviews_columns]))
        hotel_dict['reviews'] = reviews_data
        hotel_dict.update(get_hotel_tone(name))
        res = es.index(index="hotels", body=hotel_dict)

    # es.indices.refresh(index="hotels")

    # res = es.search(index="hotels", body={"query": {"match_all": {}}})
    # print("Got %d Hits:" % res['hits']['total']['value'])
    # for hit in res['hits']['hits']:
    #     print("%(name)s %(country)s: %(address)s" % hit["_source"])




if __name__ =='__main__':
    hotels_df = pd.read_csv('7282_1.csv', index_col=False)
    hotels_df.fillna('', inplace=True)
    hotels_df = hotels_df[(hotels_df['categories']=='Hotels')&(hotels_df['reviews.date']!='')]
    hotels_df.set_index('name', inplace=True, drop=False)
    app.debug=True
    app.run(host='0.0.0.0', port=3344)
    app.run(threaded=True)

