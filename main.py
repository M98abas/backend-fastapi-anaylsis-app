import uvicorn
import pandas as pd
import random
import ast
from fastapi import FastAPI, File, UploadFile,Request
from io import BytesIO
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware as starMiddle
from fastapi import Response
from fastapi.responses import JSONResponse
# import uvicorn
app = FastAPI()
import json

# if settings.BACKEND_CORS_ORIGINS:
app.add_middleware(starMiddle, allow_origins=["*"])
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Send data to /upload rather then this "}


@app.post("/uplaod")
async def result_data(file: UploadFile = File(...)):
    # try:
    # Recive file
    key_itratable = 0
    contents = file.file.read()
    
    # Convert data in the file
    data = BytesIO(contents)
    # Make the function start working rather than to set up without doing anything
    # Read file
    dfCodes = pd.read_csv("codes.csv")
    datafetched = pd.read_csv(data)
    
    for ind, dataFt in datafetched.iterrows():
        dataString = dataFt['ConnectorDetails']

        # Check if the connector doesn't contain 'ExtendedDescription' and 'clearingInstituteName'
        if 'ExtendedDescription' not in dataString and 'clearingInstituteName' not in dataString:
            result = next((item[1] for _, item in dfCodes.iterrows() if item['code_result'] == dataFt['ReturnCode']), None)
            dataNew = eval(dataFt['ConnectorDetails'])
            dataNew['ExtendedDescription'] = result
            dataNew['clearingInstituteName'] = "None"
            dataNew['key'] = key_itratable
            dataFt['ConnectorDetails'] = str(dataNew)
            key_itratable += 1
            datafetched.loc[ind] = dataFt
        elif 'ExtendedDescription' not in dataString:
            result = next((item[1] for _, item in dfCodes.iterrows() if item['code_result'] == dataFt['ReturnCode']), None)
            dataNew = eval(dataFt['ConnectorDetails'])
            dataNew['ExtendedDescription'] = result
            dataNew['key'] = key_itratable
            dataFt['ConnectorDetails'] = str(dataNew)
            key_itratable += 1
            datafetched.loc[ind] = dataFt
        # print(dateTime)
    # group data
    df2 = datafetched
    df1 = datafetched.groupby('ReturnCode').groups

    
    #print(datafetched['ReturnCode'].count())

    df2['RequestTimestamp'] = pd.to_datetime(df2["RequestTimestamp"]).dt.date

    df2['count'] = datafetched.groupby(['ReturnCode','Bin','RequestTimestamp'])['Bin'].transform('count')
    df2 = df2.drop_duplicates(subset=['ReturnCode','Bin','RequestTimestamp'])

    ConnectorDetails = pd.json_normalize(datafetched['ConnectorDetails'].apply(ast.literal_eval), max_level=1)
    ConnectorDetails = ConnectorDetails[['clearingInstituteName','ExtendedDescription']]
    ConnectorDetails['ReturnCode'] = datafetched['ReturnCode']    
    
    checks = ConnectorDetails['ExtendedDescription'].isnull()
    ConnectorDetails.loc[checks, 'ExtendedDescription'] = "None"
    ConnectorDetails = pd.DataFrame(ConnectorDetails.groupby(by=['clearingInstituteName','ExtendedDescription','ReturnCode'],as_index=False,dropna=True)['clearingInstituteName'].agg({'count':'count'}))
    # amount_sumation = pd.DataFrame(ConnectorDetails.groupby(by=['ReturnCode'],as_index=False)['Credit'].agg({'sumation':'sum'}))
    # print(amount_sumation)
    ConnectorDetail_without_null = pd.DataFrame(ConnectorDetails[~ConnectorDetails.ExtendedDescription.str.contains("None")])
    ConnectorDetails = ConnectorDetails.to_dict(orient='records')
    ConnectorDetail_without_null = ConnectorDetail_without_null.to_dict(orient='records')
    for k in range(0,len(ConnectorDetails)):
        items = ConnectorDetails[k]
        if items['ExtendedDescription'] == 'None':
            items['ExtendedDescription'] = next((item['ExtendedDescription'] for item in ConnectorDetail_without_null if item['ReturnCode'] == items['ReturnCode']),None)
            ConnectorDetails[k] = items    
    ConnectorDetails = pd.DataFrame(ConnectorDetails) 
    sumation = pd.DataFrame(ConnectorDetails.groupby(by=['clearingInstituteName'],as_index=False)['count'].agg({'sumation':'sum'}))
    keys = tuple(df1.keys())

    sumation_return_code = datafetched.pivot_table(index = ['ReturnCode'], aggfunc ='size')
    sumation_return_code = list(sumation_return_code.values)

    amount_return_code = datafetched.pivot_table(values=["Credit"], index=['ReturnCode'], aggfunc='sum').astype(float)
    amount_return_code = list(amount_return_code.values)

    # print(sumation_return_code)
    result_dict = []
    for i in range(0,len(keys)):
        keys_random = random.randint(0,10000)
        keypalce = keys[i]
        code_query_result = dfCodes.query('code_result == @keypalce')
        code_query_result = code_query_result.values[0].tolist()[1]
        bins = df2.query('ReturnCode == @keypalce')
        result_dict.append(tuple([keys_random,keypalce,code_query_result,sumation_return_code[i],amount_return_code[i],bins]))
        # print("code_query_result => ",code_query_result)
    # result_dict
    sumation.loc["Total"] = sumation['sumation'].sum()
    # sumation.append(sumation['suma'].sum().rename('Total'))
    df1 = pd.DataFrame(result_dict,columns=['key','response_code','response_description','count','total_amount','children'])
    ConnectorDetails = ConnectorDetails.to_json(orient='records')
    sumation = sumation.to_json(orient='records')
    # print(ConnectorDetails,)
    df =df1.to_json(orient='records')
    # data = str({"df1":df,"connectorDteails":ConnectorDetails,"sumation":sumation})
    # data = {df , ConnectorDetails, sumation}
    # print(dfre)
    return JSONResponse(content={'df':df, 'sumation':sumation,'connector':ConnectorDetails})
    # except:
    #     return JSONResponse(content={"e":"Error"})

    # headers = {'Content-Disposition': 'attachment; filename="data.csv"'}
    # return JSONResponse(content=jsonable_encoder(df1.to_json()))

# Stopped 
@app.post("/analize")
async def result_data(file: UploadFile = File(...)):
    try:
        # Recive file
        contents = file.file.read()
        # Convert data in the file
        data = BytesIO(contents)
        # Make the function start working rather than to set up without doing anything
        # Read file
        dfCodes = pd.read_csv("codes.csv")

        datafetched = pd.read_csv(data)

        # group data
        df1 = datafetched.groupby('ReturnCode').groups
        keys = tuple(df1.keys())

        sumation_return_code = datafetched.pivot_table(index = ['ReturnCode'], aggfunc ='size')
        sumation_return_code = list(sumation_return_code.values)

        result_dict = []

        # df = pd.DataFrame(df)
        datafetched['RequestTimestamp'] = pd.to_datetime(datafetched["RequestTimestamp"]).dt.date
        datafetched = pd.DataFrame(datafetched.groupby(by=["ReturnCode","RequestTimestamp","Bin"],as_index=False)["ReturnCode"].agg({'count':'count'}))

        for i in range(0,len(keys)):
            keypalce = keys[i]
            code_query_result = dfCodes.query('code_result == @keypalce')
            code_query_result = code_query_result.values[0].tolist()[1]

            result_dict.append(tuple([keypalce,code_query_result,sumation_return_code[i]]))

        df1 = pd.DataFrame(result_dict,columns=['response_code','response_description','Count'])
        headers = {'Content-Disposition': 'attachment; filename="data.csv"'}
        return Response(df1.to_csv(), headers=headers, media_type="text/csv")
    except:
        return JSONResponse(content={"e":"Error"})

@app.post('/reducing')
async def extracting_def(file: UploadFile = File(...)):
    # try:
    # Recive file
    contents = file.file.read()
    # Convert data in the file
    data = BytesIO(contents)
    datafetched = pd.read_csv(data)
    datafetched = datafetched.fillna('null')
    result = []
    #print(datafetched)
    for ind, rows in datafetched.iterrows():
        json_data = json.loads(rows['ConnectorDetails'])
        print(json_data)
        try:
            connector_tx_id2 = json_data.get('ConnectorTxID2', '').split('|')[1]
        except:
            connector_tx_id2 = json_data.get('ConnectorTxID2', '').split('|')[0]
        auth_code = json_data.get('AuthCode', 'Null')
        request_timestamp = rows['RequestTimestamp']
        result.append({
            'SHORT_ID': rows['ShortId'],
            'UNIQUE_ID': rows['UniqueId'],
            'RRN': connector_tx_id2,
            'AUTHH': auth_code,
            'RequestTimestamp': request_timestamp,
            'LAST_4': rows['AccountNumberLast4'],
            'BIN': rows['Bin']
        })
    return JSONResponse(content={'df': json.dumps(result)})
    # except:
    #     return JSONResponse(content={"e":"Error"})

@app.post('/merchants')
async def merchant_data(file:UploadFile = File(...)):
    key_itratable = 0
    contents = file.file.read()
    
    # Convert data in the file
    data = BytesIO(contents)
    # Make the function start working rather than to set up without doing anything
    # Read file
    dfCodes = pd.read_csv("codes.csv")
    datafetched = pd.read_csv(data)
    df = datafetched[['ChannelName', 'ReturnCode']]

    df['count'] = datafetched.groupby(['ChannelName','ReturnCode'])['ReturnCode'].transform('count')
    df = df.drop_duplicates(subset=['ChannelName','ReturnCode'])
    # print(df)
    for ind, dataFt in df.iterrows():
        result = next((item[1] for _, item in dfCodes.iterrows() if item['code_result'] == dataFt['ReturnCode']), None)
        df.at[ind, 'desc'] = result
        # Sort by 'ChannelName'
    df = df.sort_values(by=['ChannelName', 'ReturnCode'])
    df = df.to_json(orient='records')
    return JSONResponse(content={'df': df})

    
    

if __name__== '__main__':
    uvicorn.run(app,reload=True, debug=True)