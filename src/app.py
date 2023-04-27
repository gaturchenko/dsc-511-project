"""
Main script to start the web application
Run by executing `streamlit run app.py`
"""


import streamlit as st, uuid, json, redis, datetime, yaml
from loguru import logger
from kafka_utils import producer
from country_list import countries_for_language


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

st.title('LTV Prediction Tool')

@st.cache_resource
def redis_con():
    r = redis.Redis(host=config['redis']['host'], port=6379, decode_responses=True)
    return r

with st.form('params_form'):
    country_col, month_col = st.columns(2)
    with country_col:
        countries = [i[1] for i in countries_for_language('en')]
        country = st.selectbox('Choose country:', countries, countries.index('Cyprus'))
    with month_col:
        date = st.date_input('Choose the date:', datetime.date(2018, 8, 6), min_value=datetime.date(2018, 6, 12), max_value=datetime.date(2018, 11, 4))

    os_col, traffic_source_col = st.columns(2)
    with os_col:
        os = st.selectbox('Choose OS:', ['IOS', 'ANDROID'], format_func=lambda x: x.title() if x =='ANDROID' else x)
    with traffic_source_col:
        traffic_source = st.selectbox('Choose traffic source:', ['(none)', 'organic', 'cpc', 'dynamic_link', 'graphic', 'invite_a_friend_campaign', 'notification', 'rj'])
    
    send_params = st.form_submit_button('Get Prediction!')

if send_params:
    params = {
        'input_data': {
            'country': country,
            'date': date.strftime('%Y-%m-%d'),
            'os': os,
            'traffic_source': traffic_source
        },
        'request_id': str(uuid.uuid4())
    }
    message = json.dumps(params).encode('utf-8')
    producer.start_producing(message, 'app_request')
    logger.info(f'Sent message with ID {params["request_id"]} and the following parameters:\n{params["input_data"]}')

    with st.spinner('Generating your prediction...'):
        while True:
            r = redis_con()
            prediction = r.hgetall(params["request_id"])
            if 'prediction' in prediction.keys():
                r.close()
                break
            else:
                continue
    st.metric('Predicted LTV', f"{float(prediction['prediction']):.2f}")