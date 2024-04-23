import croniter
from datetime import datetime, timedelta
from config import MAILGUN
import json
from utils import (get_cron_details, service_api_call, update_api_call_details, update_log_table, dict_to_query_string,
                   send_email_via_mailgun)

while True:

    cron_details_df = get_cron_details()

    for idx, row in cron_details_df.iterrows():
        row_json = row.to_dict()

        category_id = row_json['CategoryId']
        excluded_category_ids = json.loads(row_json['ExcludedCategoryIds'])
        excluded_category_ids_lst = excluded_category_ids.get('categoryIds')
        if excluded_category_ids_lst:
            if str(category_id) in excluded_category_ids_lst:
                continue

        cron_id = row_json['CronId']
        host = row_json['HostName']
        path = row_json['Path']
        action_path = row_json['ActionPath']
        url = f'''{host}{path}{action_path}'''
        parameter_json = json.loads(row_json['ParameterJson'])
        brand_json = json.loads(row_json['BrandJson'])
        if parameter_json:
            parameter_string = dict_to_query_string(parameter_json)
            url = f'''{url}{parameter_string}'''
        if brand_json:
            brand_string = dict_to_query_string(brand_json)
            url = f'''{url}&{brand_string}'''

        expression = row_json['Cronexpression']
        expression = expression.replace('*', ' *')
        current_time = datetime.now()
        iter = croniter.croniter(expression, current_time)
        next_time = iter.get_next(datetime)
        if next_time <= current_time + timedelta(seconds=60):
            response = service_api_call(url=url, timeout=60)
            if response is None:
                send_email_via_mailgun(subject='Service Api Call Failed', auth=MAILGUN,
                                       text=url, receivers=['anuj.gaur@locobuzz.com'])
                print(f'Service Api call failed : {url}')
                continue

            upcoming_time = iter.get_next(datetime)

            try:
                end_point_response = response.json()
            except Exception as e:
                print(f'Exception in response.json() : {e}')
                end_point_response = response.content

            if response.status_code == 200:
                update_api_call_details(LastHitDate=next_time, UpcomingHitDate=upcoming_time, URLHit=url, CronID=cron_id,
                                        EndPointResponse=end_point_response, ResponseStatus=response.status_code)
            else:
                update_api_call_details(LastHitDate=next_time, UpcomingHitDate=upcoming_time, URLHit=url, CronID=cron_id,
                                        EndPointResponse=end_point_response, ResponseStatus=response.status_code)
            update_log_table(CronId=cron_id)
        else:
            continue




