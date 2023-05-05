from marshmallow_pyspark import Schema
from marshmallow import fields, validate
from country_list import countries_for_language


class LTVSchema(Schema):
    '''
    Typization schema for ltv data frame \n
    I.e. the `df_tv` object used in `src/prediction/predictor.py` script
    '''

    ### ----------- Individual Variables Typization ----------- ###

    first_touch_date = fields.Date(
        format='%Y-%m-%d',
        allow_none=False,
        error_messages={
            'null': 'Field may not be null.',
            'validator_failed': 'Date must be in "%Y-%m-%d" format'
        },
        required=True
    )
    traffic_source = fields.String(allow_none=False)
    # More sophisticated typization of `fields.String` due to a very limited number of options
    os = fields.String(
        allow_none=False,
        validate=validate.OneOf(
            choices=['ANDROID', 'IOS'],
            error='OS must be one of [{choices}]'
        ),
        required=True
    )
    # More sophisticated typization of `fields.String` to preserve coherence between web app input and other potential services
    country = fields.String(
        allow_none=False,
        # Same list of countries which is supplied to the widget in UI
        validate=validate.OneOf(
            choices=[country_tuple[1] for country_tuple in countries_for_language('en')],
            error='''Requested country must be one of the following:
```
from country_list import countries_for_language
print([i[1] for i in countries_for_language('en')])
```'''
        ),
        required=True
    )
    cohort_ltv_avg_lifetime = fields.Number(
        allow_nan=False,
        validate=validate.Range(
            min=0,
            min_inclusive=True,
            error='LTV must be greater than or equal to {min}'
        ),
        required=True
    )
