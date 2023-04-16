import streamlit as st, uuid, json, redis
from loguru import logger
from kafka_utils import producer


st.title('LTV Prediction Tool')

@st.cache_resource
def redis_con():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r

with st.form('params_form'):
    country_col, month_col = st.columns(2)
    with country_col:
        country = st.selectbox('Choose country:', ['Cyprus', 'USA', 'UK'])
    with month_col:
        month = st.selectbox('Choose month:', ['August', 'June', 'July'])

    os_col, traffic_source_col = st.columns(2)
    with os_col:
        os = st.selectbox('Choose OS:', ['IOS', 'Android'])
    with traffic_source_col:
        traffic_source = st.selectbox('Choose traffic source:', ['(none)', 'organic', 'cpc'])
    
    send_params = st.form_submit_button('Get Prediction!')

if send_params:
    params = {
        'input_data': {
            'country': country,
            'month': month,
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