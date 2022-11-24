import pandas as pd
import random
from fastapi import FastAPI, File, UploadFile
from io import BytesIO
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
# import uvicorn
app = FastAPI(debug=True)

# routes = ...

# middleware = [Middleware(CORSMiddleware, allow_origins=['*'])]

# app = Starlette(routes=routes, middleware=middleware)
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
    # print(request)
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
    df2 = pd.DataFrame(datafetched.groupby(['ReturnCode','AccountNumberLast4','UniqueId','AccountHolder','RequestTimestamp','ChannelName','Currency','Credit','ConnectorDetails'],as_index=False)['Bin'].agg({'count':'count'}))
    print(df2)
    keys = tuple(df1.keys())

    sumation_return_code = datafetched.pivot_table(index = ['ReturnCode'], aggfunc ='size')
    sumation_return_code = list(sumation_return_code.values)
    print(sumation_return_code)
    result_dict = []
    for i in range(0,len(keys)):
        keys_random = random.randint(0,10000)
        keypalce = keys[i]
        code_query_result = dfCodes.query('code_result == @keypalce')
        code_query_result = code_query_result.values[0].tolist()[1]
        bins = df2.query('ReturnCode == @keypalce')
        result_dict.append(tuple([keys_random,keypalce,code_query_result,sumation_return_code[i],bins]))
        # print(keypalce)
    # result_dict
    df1 = pd.DataFrame(result_dict,columns=['key','response_code','response_description','count','children'])

    df =df1.to_json(orient='records')
    return Response(df, media_type="text/json")


    # headers = {'Content-Disposition': 'attachment; filename="data.csv"'}
    # return JSONResponse(content=jsonable_encoder(df1.to_json()))

@app.post("/analize")
async def result_data(file: UploadFile = File(...)):
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

