from fastapi import FastAPI, File, UploadFile
from io import BytesIO
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response
import pandas as pd
# import uvicorn
app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

"""
    def read_return_data(data_file):
    # Read file
    dfCodes = pd.read_csv("codes.csv")
    datafetched = pd.read_csv(data_file)
    # group data
    df1 = datafetched.groupby('ReturnCode').groups
    keys = df1.keys()
    sumation = datafetched.pivot_table(index = ['ReturnCode'], aggfunc ='size')
    sumation = dict(sumation)
    result_dict = []
    for i in keys:
        code_query_result = dfCodes.query('code_result == @i')
        code_query_result = code_query_result.values[0].tolist()[1]
        result_dict.append(tuple([i,code_query_result,sumation[i]]))
    return result_data"""


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
    keys = tuple(df1.keys())

    sumation = datafetched.pivot_table(index = ['ReturnCode'], aggfunc ='size')
    sumation = list(sumation.values)
    result_dict = []

    for i in range(0,len(keys)):
        keypalce = keys[i]
        code_query_result = dfCodes.query('code_result == @keypalce')
        code_query_result = code_query_result.values[0].tolist()[1]

        result_dict.append(tuple([keypalce,code_query_result,sumation[i]]))

    df1 = pd.DataFrame(result_dict)
    headers = {'Content-Disposition': 'attachment; filename="data.csv"'}
    return Response(df1.to_csv(), headers=headers, media_type="text/csv")


    # return JSONResponse(content=jsonable_encoder(df1.to_json()))