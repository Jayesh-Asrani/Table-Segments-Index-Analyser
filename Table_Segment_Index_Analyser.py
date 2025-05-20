import requests
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")


def table_details():
    # List out the Segments of the table
    tableSizeAPIUrl = contorllerUrl + '/tables/' + baseTableName + '/size'

    tableSizeResponseJson = requests.get(tableSizeAPIUrl, headers=headers).json()

    if tableType == 'OFFLINE':
        segments_data = tableSizeResponseJson["offlineSegments"]["segments"]
    else:
        segments_data = tableSizeResponseJson["realtimeSegments"]["segments"]

    tableSizeWithoutReplication = tableSizeResponseJson["reportedSizePerReplicaInBytes"] / 1000000
    tableSizeWithReplication = tableSizeResponseJson["reportedSizeInBytes"] / 1000000
    totalNumberSegments = len(segments_data)
    avgSegmentSize = tableSizeWithoutReplication / totalNumberSegments

    for segment_name, data in segments_data.items():
        segmentsList.append((segment_name, data["maxReportedSizePerReplicaInBytes"] / 1000000))

    segments = pd.DataFrame(segmentsList, columns=["segmentName", "Size"])

    segments.sort_values(by=['Size'])

    st.session_state['segmentsDF'] = segments

    segmentSizeHistogramList = [segments[segments['Size'] < 100]['segmentName'].count(),
                                segments[(segments['Size'] > 100) & (segments['Size'] <= 200)]['segmentName'].count(),
                                segments[(segments['Size'] > 200) & (segments['Size'] <= 300)]['segmentName'].count(),
                                segments[(segments['Size'] > 300) & (segments['Size'] <= 400)]['segmentName'].count(),
                                segments[(segments['Size'] > 400) & (segments['Size'] <= 500)]['segmentName'].count(),
                                segments[(segments['Size'] > 500) & (segments['Size'] <= 600)]['segmentName'].count(),
                                segments[(segments['Size'] > 600) & (segments['Size'] <= 700)]['segmentName'].count(),
                                segments[(segments['Size'] > 700)]['segmentName'].count()]

    segmentSizeHistogramLabel = ["< 100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-700", "> 700"]

    segmentSizeHistogram = pd.DataFrame(segmentSizeHistogramList, columns=["Count"])
    segmentSizeHistogram['Segment Size'] = segmentSizeHistogramLabel

    st.markdown('#')

    st.subheader('Table Metrics')

    metric1, metric2, metric3, metric4 = st.columns(4)

    metric1.metric(
        label="Table Size without Replication",
        value=str(round(tableSizeWithoutReplication / 1000, 2)) + " Gb"
    )

    metric2.metric(
        label="Table Size with Replication",
        value=str(round(tableSizeWithReplication / 1000, 2)) + " Gb"
    )

    metric3.metric(
        label="Number of Segments",
        value=totalNumberSegments
    )

    metric4.metric(
        label="Average Segment Size",
        value=str(round(avgSegmentSize, 2)) + " Mb"
    )

    st.markdown('#')

    st.subheader("Segment Size Distribution")

    st.bar_chart(segmentSizeHistogram, use_container_width=True, x='Segment Size', y='Count', height=500)


def segment_details():
    segmentMetadataAPIUrl = contorllerUrl + '/segments/' + baseTableName + '/metadata'

    segmentsMetadataResponse = requests.get(segmentMetadataAPIUrl, headers=headers).json()

    segmentsMetadata = pd.DataFrame.from_dict(segmentsMetadataResponse, orient='index')

    segmentsMetadata = segmentsMetadata.drop('star-tree-index', axis=1)
    segmentsMetadata = segmentsMetadata.drop('schemaName', axis=1)
    segmentsMetadata = segmentsMetadata.drop('crc', axis=1)
    segmentsMetadata = segmentsMetadata.drop('creationTimeMillis', axis=1)
    segmentsMetadata = segmentsMetadata.drop('timeUnit', axis=1)
    segmentsMetadata = segmentsMetadata.drop('timeGranularitySec', axis=1)
    segmentsMetadata = segmentsMetadata.drop('timeColumn', axis=1)
    segmentsMetadata = segmentsMetadata.drop('startTimeMillis', axis=1)
    segmentsMetadata = segmentsMetadata.drop('endTimeMillis', axis=1)
    segmentsMetadata = segmentsMetadata.drop('segmentVersion', axis=1)
    segmentsMetadata = segmentsMetadata.drop('creatorName', axis=1)
    segmentsMetadata = segmentsMetadata.drop('custom', axis=1)
    segmentsMetadata = segmentsMetadata.drop('startOffset', axis=1)
    segmentsMetadata = segmentsMetadata.drop('endOffset', axis=1)
    segmentsMetadata = segmentsMetadata.drop('columns', axis=1)
    segmentsMetadata = segmentsMetadata.drop('indexes', axis=1)

    segmentsMetadata.reset_index(drop=True, inplace=True)

    totalDocs = segmentsMetadata['totalDocs'].sum()
    maxDocumentsInSegment = segmentsMetadata['totalDocs'].max()
    minDocumentsInSegment = segmentsMetadata['totalDocs'].min()

    st.subheader('Segment Metrics')

    metric1, metric2, metric3 = st.columns(3)

    metric1.metric(
        label="Total Documents",
        value='{:,}'.format(totalDocs)
    )

    metric2.metric(
        label="Max Docs in Segment",
        value='{:,}'.format(maxDocumentsInSegment)
    )

    metric3.metric(
        label="Min Docs in Segment",
        value='{:,}'.format(minDocumentsInSegment)
    )

    st.markdown('#')

    segmentsMetadata = segmentsMetadata.merge(st.session_state.segmentsDF, on='segmentName', how='left')

    st.subheader("Segment Details Table")

    # segmentsMetadata = segmentsMetadata.set_index('columnName')

    st.dataframe(data=segmentsMetadata, use_container_width=True, height=500, hide_index=True)


def index_column_details():
    st.subheader("Index-Column Details")

    segmentDetailsDF = st.session_state.segmentsDF
    defaultSegmentName = segmentDetailsDF['segmentName'][0]
    segmentName = st.text_input("Enter Segment Name",
                                value=defaultSegmentName)

    segmentMetadataAPIUrl = contorllerUrl + '/segments/' + baseTableName + '/' + segmentName + '/metadata?columns=*'

    segmentMetadaResponseJson = requests.get(segmentMetadataAPIUrl, headers=headers).json()["columns"]

    segmentsMetadata = pd.DataFrame.from_dict(segmentMetadaResponseJson)

    segmentsMetadata = segmentsMetadata.drop('partitionFunction', axis=1)
    segmentsMetadata = segmentsMetadata.drop('totalNumberOfEntries', axis=1)
    segmentsMetadata = segmentsMetadata.drop('partitions', axis=1)
    segmentsMetadata = segmentsMetadata.drop('maxNumberOfMultiValues', axis=1)
    segmentsMetadata = segmentsMetadata.drop('columnMaxLength', axis=1)

    segmentsMetadata = segmentsMetadata.drop('minMaxValueInvalid', axis=1)
    segmentsMetadata = segmentsMetadata.drop('bitsPerElement', axis=1)
    segmentsMetadata = segmentsMetadata.drop('autoGenerated', axis=1)
    segmentsMetadata = segmentsMetadata.drop('fieldSpec', axis=1)

    indexSizeMap = segmentsMetadata['indexSizeMap'].apply(pd.Series)
    indexSizeMap = indexSizeMap.div(1000000)

    segmentsMetadata = segmentsMetadata.merge(indexSizeMap, left_index=True, right_index=True, how='left')

    segmentsMetadata = segmentsMetadata.drop('indexSizeMap', axis=1)

    st.markdown('#')

    metric1, metric2, metric3 = st.columns(3)

    totalDocs = segmentsMetadata['totalDocs'].max()
    segmentSize = segmentDetailsDF.loc[segmentDetailsDF['segmentName'] == segmentName, 'Size']
    maxSizeofDict = segmentsMetadata['dictionary'].max()
    maxSizeDictName = segmentsMetadata[segmentsMetadata['dictionary'] == maxSizeofDict]['columnName'].values[0]
    segmentsMetadata = segmentsMetadata.drop('totalDocs', axis=1)

    metric1.metric(
        label="Total Documents",
        value='{:,}'.format(totalDocs)
    )

    metric2.metric(
        label="Segment Size - (Mb)",
        value=segmentSize
    )

    metric3.metric(
        label="Column Name - Max Size Dict",
        value=maxSizeDictName + " - " + '{:,}'.format((maxSizeofDict))
    )

    st.markdown('#')
    segmentsMetadata = segmentsMetadata.set_index('columnName')

    st.subheader("Index-Column Details Table")

    st.dataframe(data=segmentsMetadata, height=600, hide_index=False, use_container_width=True)


def segment_server_details():
    serverSegmentMapAPIUrl = contorllerUrl + '/segments/' + baseTableName + '/servers?type=' + tableType

    serverSegmentMapResponseJson = requests.get(serverSegmentMapAPIUrl, headers=headers).json()[0][
        "serverToSegmentsMap"]

    serverSegmentMapList = []

    for server_name, data in serverSegmentMapResponseJson.items():
        server_name = server_name[13:server_name.index('.')]
        serverSegmentMapList.append((server_name, len(data)))

    segmentServerMap = pd.DataFrame(serverSegmentMapList, columns=["Servers", "Segment Count"])

    st.markdown('#')

    st.subheader("Server Segment Distribution")

    st.bar_chart(segmentServerMap, use_container_width=True, x='Servers', y='Segment Count', height=500)


st.header("Table - Segments - Index Overview")

st.markdown('#')

contorllerUrl = st.text_input("Enter Controller URL", value='https://pinot.saas.demo.startree.cloud')
token = st.text_input("Enter Pinot token",
                      value='Basic YjhiMjhkNWFmNTY1NDdlODhkZWY4MTYwNTk1ODk4YWQ6MXRwNVZGRjJxZ2pVMnA5NFNkNXBNUklDVnlSZmQwQ0R2NVNFTGF5bTNLUT0=')

headers = {
    "Authorization": token,
    'accept': 'application/json'
}

segmentsList = []

listTablesAPIUrl = contorllerUrl + '/tables?type='

offlineTables = requests.get(listTablesAPIUrl + "offline", headers=headers).json()['tables']
realtimeTables = requests.get(listTablesAPIUrl + "realtime", headers=headers).json()['tables']

tables = offlineTables + realtimeTables

baseTableName = st.selectbox("Select Table", tables, index=0)

tableType = baseTableName[baseTableName.rfind('_') + 1:]

table_details()
segment_details()
segment_server_details()
index_column_details()
