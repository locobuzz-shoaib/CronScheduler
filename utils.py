import httpx
import requests

from config import ENVIRON, LOG_ENABLED, GCHAT_WEBHOOK
from database import execute

ASYNC_CLIENT = httpx.AsyncClient(verify=False)


def get_cron_details():
    try:
        query = f'''Select CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId
                    ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive
                    ,CategoryId ,InsertedDate,updateddate ,HitStartDate ,HitEndDate
                    ,EndPointResponse, ResponseStatus, UpdatedBy
                    From Spatialrss.dbo.CronJobScheduleDetail'''
        data = execute(query)
        return data
    except Exception as ex:
        send_message_to_google_chat(f"Error in get cron job: {ex}")


async def service_api_call(url, timeout):
    retry = 0
    while retry < 4:
        try:
            response = requests.get(url, timeout=timeout)
            return response
        except Exception as e:
            print(f'Exception to get_html_response Retry count : {retry} for url : {url}......{e}')
            retry += 1
    return None


async def update_api_call_details(LastHitDate, UpcomingHitDate, URLHit, CronID, EndPointResponse, ResponseStatus, UpdatedBy):
    try:
        query = f'''Update Spatialrss.dbo.CronJobScheduleDetail
                Set LastHitDate = '{LastHitDate}', UpcomingHitDate = '{UpcomingHitDate}',
                    URLHit = '{URLHit}', updateddate = GETUTCDATE(),UpdatedBy = '{UpdatedBy}',
                    HitStartDate = '{LastHitDate}',  HitEndDate = '{LastHitDate}',
                    EndPointResponse = '{EndPointResponse}', ResponseStatus = '{ResponseStatus}'
                Where CronID = {CronID} '''
        await execute(query, True)
    except Exception as ex:
        await send_message_to_google_chat(f"Error in update_api_call_details function: {ex}")



async def update_log_table(CronId):
    try:
        query = f'''Insert into Spatialrss.dbo.CronJobScheduleDetail_log
            (CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId
            ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive ,CategoryId ,InsertedDate,InsertedBy
            ,updateddate ,UpdatedBy ,HitStartDate ,HitEndDate,EndPointResponse, ResponseStatus,LoggedDate)
            Select CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId
            ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive ,CategoryId ,InsertedDate,InsertedBy
            ,updateddate ,UpdatedBy ,HitStartDate ,HitEndDate,EndPointResponse, ResponseStatus,GETUTCDATE()
            From Spatialrss.dbo.CronJobScheduleDetail With(Nolock)
            Where CronId={CronId}'''
        await execute(query, True)
    except Exception as ex:
        await send_message_to_google_chat(f"Error in update log query function: {ex}")


async def dict_to_query_string(d):
    query_string = ""
    for key, value in d.items():
        if query_string:
            query_string += "&"
        query_string += f"{key}={value}"
    return query_string


async def send_email_via_mailgun(subject: str, text: str, auth: str, file_path: str = '',
                           receivers: list = ['shweta.singh@locobuzz.com', 'akshar.ganatra@locobuzz.com'],
                           sender: str = "Locobuzz",
                           html: str = ''):
    try:
        email_params = dict(url="https://api.mailgun.net/v3/locobuzz.info/messages",
                            auth=("api", auth),
                            data={"from": f"%s <no-reply@locobuzz.com>" % sender,
                                  "to": receivers,
                                  "subject": subject,
                                  "text": text,
                                  "html": html})

        response = requests.post(**email_params)
        return response
    except Exception as ex:
        await send_message_to_google_chat(f"Error in send mail function: {ex}")


async def send_message_to_google_chat(text_message):
    """Hangouts Chat incoming webhook quickstart."""
    try:
        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
        bot_message = {"cards": [{"header": {"title": f"*TIKTOK COMMENT*", "subtitle": f"{ENVIRON}"},
                                  "sections": [{"widgets": [{"textParagraph": {"text": f"{text_message}"}}]}]}]}
        if ENVIRON.lower() in LOG_ENABLED:
            if GCHAT_WEBHOOK:
                await ASYNC_CLIENT.post(url=GCHAT_WEBHOOK, headers=message_headers, json=bot_message)
        print(text_message)
    except Exception as e:
        print('Error in webhookApi', e)

