import urllib3
import pandas as pd
import datetime as dt
from requests_kerberos import OPTIONAL, HTTPKerberosAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import requests
import io
import sys
import json



urllib3.disable_warnings()

def requests_retry_session(retries=10,
                           backoff_factor=0.3,
                           status_forcelist=(500, 502, 503, 504),
                           session=None):

    session = session or requests.Session()

    retry = Retry(total=retries,
                  read=retries,
                  connect=retries,
                  backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def pull_rodeo_csv(url):
    resp = requests_retry_session().get(url,
                                    auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
                                    verify=False,
                                    allow_redirects=True,
                                    timeout=30)

    if resp.status_code == 200:

        csv_data = resp.content

        if csv_data is not None:
            
            rawData = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
            df = rawData.dropna(axis=0, thresh=4)
            

        else:
            print("No Data")
    
    else:
        print(resp.raise_for_status())

    return df

def get_nextCPT (df):
    ExSD_list = ["02:30","03:00","03:30","06:45","10:45","14:30","18:45","19:15","19:45","23:00","23:30"]
    df['ExSD'] = pd.to_datetime(df['ExSD'])
    df['ExSD_str'] = df['ExSD'].dt.strftime('%H:%M')
    df = df[df['ExSD_str'].isin(ExSD_list)]
    now = dt.datetime.now()
    minT = df['ExSD'].min()
    
    PADtime = ((minT - now)- pd.Timedelta(minutes=20))
    PADtime = PADtime.total_seconds()/60
    nextCPT = df.loc[df['ExSD'] == minT]['ExSD_str'].min()
    
    return PADtime,nextCPT

def post_message (msg):
    response = None
    try:
        response = requests.post(
            url = webhook,
            json = {"Content": msg})
        return json.loads(response.text)
    except:
        return response.text

webhook = "https://hooks.chime.aws/incomingwebhooks/44de0f7c-c1d8-4c67-92ae-afd5cdc31988?token=ODd4U2dVQ3B8MXxETE56bkh0cE5JQzZzTGdUOThyTXZMQndRWU1YVzNHUzNRXzlaejhSX2Yw"

shipment_thrs = 30

fc = "MAD4"
url1 = f"https://rodeo-dub.amazon.com/{fc}/ItemListCSV?ShipmentId=&ChargeRange.RangeStartMillis=&ScannableId=&ShipMethod=&WorkPool=ReadyToPickUnconstrained&EulerGroupType=&FcSku=&Label=&IsEulerPromiseMiss=ALL&StackingFilter=&GiftOption=ALL&NextDestination=&shipmentType=CUSTOMER_SHIPMENTS&Trailer=&ShipOption=&OuterScannableId=&SspState=&ChargeRange.RangeEndMillis=&OuterOuterScannableId=&FnSku=&_enabledColumns=on&Condition=&DwellTimeLessThan=0&DwellTimeLessThan=&OuterOuterContainerLabel=&ExSDRange.RangeStartMillis=1676411999999&LastExSDRange.RangeStartMillis=&DwellTimeGreaterThan=0&DwellTimeGreaterThan=&ExSDRange.RangeEndMillis=1677059160000&Fracs=ALL&PickBatchId=&SortCode=&DestinationWarehouseId=&ProcessPath=PPMultiMedium%2CPPMultiWrap&IsReactiveTransfer=ALL&IsEulerUpgraded=ALL&OuterContainerType=&PickPriority=&enabledColumns=OUTER_SCANNABLE_ID&FulfillmentServiceClass=ALL&LastExSDRange.RangeEndMillis=&IsEulerExSDMiss=ALL&FulfillmentReferenceId=&OuterOuterContainerType="
url2 = f"https://rodeo-dub.amazon.com/{fc}/ItemListCSV?ShipmentId=&ChargeRange.RangeStartMillis=&ScannableId=&ShipMethod=&WorkPool=PickingNotYetPicked&EulerGroupType=&FcSku=&Label=&IsEulerPromiseMiss=ALL&StackingFilter=&GiftOption=ALL&NextDestination=&shipmentType=CUSTOMER_SHIPMENTS&Trailer=&ShipOption=&OuterScannableId=&SspState=&ChargeRange.RangeEndMillis=&OuterOuterScannableId=&FnSku=&_enabledColumns=on&Condition=&DwellTimeLessThan=0&DwellTimeLessThan=&OuterOuterContainerLabel=&ExSDRange.RangeStartMillis=1676424599999&LastExSDRange.RangeStartMillis=&DwellTimeGreaterThan=0&DwellTimeGreaterThan=&ExSDRange.RangeEndMillis=1676886360000&Fracs=ALL&PickBatchId=&SortCode=&DestinationWarehouseId=&ProcessPath=PPMultiMedium%2CPPMultiWrap&IsReactiveTransfer=ALL&IsEulerUpgraded=ALL&OuterContainerType=&PickPriority=&enabledColumns=OUTER_SCANNABLE_ID&FulfillmentServiceClass=ALL&LastExSDRange.RangeEndMillis=&IsEulerExSDMiss=ALL&FulfillmentReferenceId=&OuterOuterContainerType="
url3 = f"https://rodeo-dub.amazon.com/{fc}/ItemListCSV?ShipmentId=&ChargeRange.RangeStartMillis=&ScannableId=&ShipMethod=&WorkPool=PickingPicked&EulerGroupType=&FcSku=&Label=&IsEulerPromiseMiss=ALL&StackingFilter=&GiftOption=ALL&NextDestination=&shipmentType=CUSTOMER_SHIPMENTS&Trailer=&ShipOption=&OuterScannableId=&SspState=&ChargeRange.RangeEndMillis=&OuterOuterScannableId=&FnSku=&_enabledColumns=on&Condition=&DwellTimeLessThan=0&DwellTimeLessThan=&OuterOuterContainerLabel=&ExSDRange.RangeStartMillis=1676413799999&LastExSDRange.RangeStartMillis=&DwellTimeGreaterThan=0&DwellTimeGreaterThan=&ExSDRange.RangeEndMillis=1677205860000&Fracs=ALL&PickBatchId=&SortCode=&DestinationWarehouseId=&ProcessPath=PPMultiMedium%2CPPMultiWrap&IsReactiveTransfer=ALL&IsEulerUpgraded=ALL&OuterContainerType=&PickPriority=&enabledColumns=OUTER_SCANNABLE_ID&FulfillmentServiceClass=ALL&LastExSDRange.RangeEndMillis=&IsEulerExSDMiss=ALL&FulfillmentReferenceId=&OuterOuterContainerType="
url4 = f"https://rodeo-dub.amazon.com/{fc}/ItemListCSV?ShipmentId=&ChargeRange.RangeStartMillis=&ScannableId=&ShipMethod=&WorkPool=RebinBuffered&EulerGroupType=&FcSku=&Label=&IsEulerPromiseMiss=ALL&StackingFilter=&GiftOption=ALL&NextDestination=&shipmentType=CUSTOMER_SHIPMENTS&Trailer=&ShipOption=&OuterScannableId=&SspState=&ChargeRange.RangeEndMillis=&OuterOuterScannableId=&FnSku=&_enabledColumns=on&Condition=&DwellTimeLessThan=0&DwellTimeLessThan=&OuterOuterContainerLabel=&ExSDRange.RangeStartMillis=1676413799999&LastExSDRange.RangeStartMillis=&DwellTimeGreaterThan=0&DwellTimeGreaterThan=&ExSDRange.RangeEndMillis=1676972760000&Fracs=ALL&PickBatchId=&SortCode=&DestinationWarehouseId=&ProcessPath=PPMultiMedium%2CPPMultiWrap&IsReactiveTransfer=ALL&IsEulerUpgraded=ALL&OuterContainerType=&PickPriority=&enabledColumns=OUTER_SCANNABLE_ID&FulfillmentServiceClass=ALL&LastExSDRange.RangeEndMillis=&IsEulerExSDMiss=ALL&FulfillmentReferenceId=&OuterOuterContainerType="
url5 = f"https://rodeo-dub.amazon.com/{fc}/ItemListCSV?ShipmentId=&ChargeRange.RangeStartMillis=&ScannableId=&ShipMethod=&WorkPool=Sorted&EulerGroupType=&FcSku=&Label=&IsEulerPromiseMiss=ALL&StackingFilter=&GiftOption=ALL&NextDestination=&shipmentType=CUSTOMER_SHIPMENTS&Trailer=&ShipOption=&OuterScannableId=&SspState=&ChargeRange.RangeEndMillis=&OuterOuterScannableId=&FnSku=&_enabledColumns=on&Condition=&DwellTimeLessThan=0&DwellTimeLessThan=&OuterOuterContainerLabel=&ExSDRange.RangeStartMillis=1676411999999&LastExSDRange.RangeStartMillis=&DwellTimeGreaterThan=0&DwellTimeGreaterThan=&ExSDRange.RangeEndMillis=1676899860000&Fracs=ALL&PickBatchId=&SortCode=&DestinationWarehouseId=&ProcessPath=PPMultiMedium%2CPPMultiWrap&IsReactiveTransfer=ALL&IsEulerUpgraded=ALL&OuterContainerType=&PickPriority=&enabledColumns=OUTER_SCANNABLE_ID&FulfillmentServiceClass=ALL&LastExSDRange.RangeEndMillis=&IsEulerExSDMiss=ALL&FulfillmentReferenceId=&OuterOuterContainerType="

ls_url = [url1,url2,url3,url4,url5]

def join_rodeo (url_list):
    conc_df = pd.DataFrame()
    for i in url_list:
       df = pull_rodeo_csv(i)
       conc_df = pd.concat([conc_df, df], ignore_index=True)
    return conc_df

def detect_ls (df):
    df= df.loc[(rodeo['Quantity'] > shipment_thrs) & (rodeo['Ship Option'] != "vendor-returns")].sort_values(by=['Quantity'],ascending=False).reset_index(drop=True)
    df = df[['Shipment ID', 'FN SKU', 'Quantity','Expected Ship Date']]
    return df

rodeo = join_rodeo(ls_url)
rodeo = detect_ls(rodeo)


if not rodeo.empty:

    rodeo['Shipment ID'] = rodeo['Shipment ID'].apply(lambda x: f"[{x}](https://rodeo-dub.amazon.com/MAD4/Search?_enabledColumns=on&enabledColumns=ASIN_TITLES&enabledColumns=OUTER_SCANNABLE_ID&searchKey={x})")
    #rodeo['FN SKU'] = rodeo['FN SKU'].apply(lambda x: f"[{x}](http://fcresearch-eu.aka.amazon.com/MAD4/results?s={x}&profile=0b7e2a48-2d1f-496c-95f3-f867a9566bc1)")

    t = rodeo.to_markdown(tablefmt="github", index=False)
    msg = (f"/md @Present\n ## Large Shipment Alert:\n {t}")
    post_message(msg)

else:
    msg = "/md **No call-outs**"
    post_message(msg)
    

