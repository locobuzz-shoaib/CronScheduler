import requests
from database import MssqlHandler

sql_conn_obj = MssqlHandler("rw")


def get_cron_details():
    query = f'''Select CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId 
                ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive 
                ,CategoryId ,InsertedDate,updateddate ,HitStartDate ,HitEndDate 
                ,EndPointResponse, ResponseStatus
                From Spatialrss.dbo.CroneJobScheduleDetails'''
    sql_conn_obj.execute(query)
    df = sql_conn_obj.fetch_df()
    return df


def service_api_call(url, timeout):
    retry = 0
    while retry < 4:
        try:
            response = requests.get(url, timeout=timeout)
            return response
        except Exception as e:
            print(f'Exception to get_html_response Retry count : {retry} for url : {url}......{e}')
            retry += 1
    return None


def update_api_call_details(LastHitDate, UpcomingHitDate, URLHit, CronID, EndPointResponse, ResponseStatus):
    query = f'''Update Spatialrss.dbo.CroneJobScheduleDetails
                Set LastHitDate = '{LastHitDate}', UpcomingHitDate = '{UpcomingHitDate}',
                    URLHit = '{URLHit}', updateddate = GETUTCDATE(),
                    HitStartDate = '{LastHitDate}',  HitEndDate = '{LastHitDate}',
                    EndPointResponse = '{EndPointResponse}', ResponseStatus = '{ResponseStatus}'
                Where CronID = {CronID} '''
    sql_conn_obj.execute(query)
    sql_conn_obj.commit()


def update_log_table(CronId):
    query = f'''Insert into Spatialrss.dbo.CroneJobScheduleDetails_log
            (CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId 
            ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive ,CategoryId ,InsertedDate,updateddate ,HitStartDate ,HitEndDate
            ,EndPointResponse, ResponseStatus)
            Select CronId,HostName ,Path ,ActionPath ,ParameterJson ,ExcludedCategoryIds ,BrandJson ,ChannelGroupId 
            ,LastHitDate ,UpcomingHitDate ,Cronexpression ,URLHit ,ServiceName ,IsActive ,CategoryId ,InsertedDate,updateddate ,HitStartDate ,HitEndDate 
            ,EndPointResponse, ResponseStatus
            From Spatialrss.dbo.CroneJobScheduleDetails With(Nolock)
            Where CronId={CronId}'''
    sql_conn_obj.execute(query)
    sql_conn_obj.commit()


def dict_to_query_string(d):
    query_string = ""
    for key, value in d.items():
        if query_string:
            query_string += "&"
        query_string += f"{key}={value}"
    return query_string


def send_email_via_mailgun(subject: str, text: str, auth: str, file_path: str = '',
                           receivers: list = ['anuj.gaur@locobuzz.com'],
                           sender: str = "Locobuzz",
                           html: str = ''):
    email_params = dict(url="https://api.mailgun.net/v3/locobuzz.info/messages",
                        auth=("api", auth),
                        data={"from": f"%s <no-reply@locobuzz.com>" % sender,
                              "to": receivers,
                              "subject": subject,
                              "text": text,
                              "html": html})

    response = requests.post(**email_params)
    return response

