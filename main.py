import asyncio
import json
from datetime import datetime, timedelta

import aiohttp
import croniter

from config import MAILGUN
from database import create_pool_instance
from utils import (dict_to_query_string, send_email_via_mailgun, update_api_call_details, update_log_table,
                   get_cron_details, send_message_to_google_chat)


async def make_api_call(session, url):
    async with session.get(url) as response:
        return response.status


async def cron_job_process():
    try:
        loop = asyncio.get_running_loop()
        await create_pool_instance(loop, "rw")
        while True:
            cron_details_df = await get_cron_details()

            for idx, row in cron_details_df.iterrows():
                row_json = row.to_dict()
                print(row_json['CategoryId'])
                category_id = row_json['CategoryId']
                if row_json.get('ExcludedCategoryIds') is not None:
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
                updated_by = row_json['UpdatedBy']

                if row_json['BrandJson']:
                    brand_json = json.loads(row_json['BrandJson'])
                    included_brands = brand_json.get('IncludedBrandId', [])  # Extract the list from 'IncludedBrandId'
                    converted_brands = [int(brand) for brand in included_brands]
                    print(converted_brands)
                    if parameter_json:
                        parameter_string = await dict_to_query_string(parameter_json)
                        url = f'''{url}{parameter_string}'''
                    brand_string = await dict_to_query_string(brand_json)
                    url = f'''{url}&{brand_string}'''

                expression = row_json['Cronexpression']
                expression = expression.replace('*', ' *')
                current_time = datetime.now()
                iter = croniter.croniter(expression, current_time)
                next_time = iter.get_next(datetime)
                if not next_time <= current_time + timedelta(seconds=60):

                    async with aiohttp.ClientSession() as session:
                        tasks = make_api_call(session, url)
                        responses = await asyncio.gather(tasks)
                        responses = responses[0]
                        print(f"Response from {url}, response: {responses}")

                        if responses != 200:
                            await send_email_via_mailgun(subject='Service Api Call Failed', auth=MAILGUN,
                                                         text=url, receivers=['shweta.singh@locobuzz.com'])
                            print(f'Service Api call failed : {url}')
                            continue

                        upcoming_time = iter.get_next(datetime)

                        try:
                            end_point_response = responses
                        except Exception as e:
                            print(f'Exception in response.json() : {e}')
                            end_point_response = responses

                        if responses:
                            await update_api_call_details(LastHitDate=next_time, UpcomingHitDate=upcoming_time,
                                                          URLHit=url,
                                                          CronID=cron_id,
                                                          EndPointResponse=end_point_response, ResponseStatus=responses,
                                                          UpdatedBy=updated_by)
                        else:
                            await update_api_call_details(LastHitDate=next_time, UpcomingHitDate=upcoming_time,
                                                          URLHit=url,
                                                          CronID=cron_id,
                                                          EndPointResponse=end_point_response, ResponseStatus=responses,
                                                          UpdatedBy=updated_by)
                        await update_log_table(CronId=cron_id)
                        print(f'log table updated')
    except Exception as ex:
        await send_message_to_google_chat(f"Error in cron job process: {ex}")


if __name__ == '__main__':
    asyncio.run(cron_job_process())
