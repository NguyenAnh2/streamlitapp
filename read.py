import json
import time
import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
import requests
from logconfig import LogConfigSQL, LogConfigExcel
from requests.auth import HTTPBasicAuth

def get_csrf_token():
    try:
        csrf_url = "http://172.28.136.105:8080/api/v1/dags/CREATE_DAG"
        auth = HTTPBasicAuth('admin', 'KQXMuEEpxthWmk75')
        response = requests.get(csrf_url, auth=auth)
        st.write(response.status_code)
        if response.status_code == 200:
            return response.headers.get("X-CSRF-Token")
        else:
            st.error(f"Failed to get CSRF token. Status code: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def api_request(method, url, json=None):
    headers = {
        "Content-Type": "application/json",
        "X-CSRF-Token": get_csrf_token()
    }
    auth = HTTPBasicAuth('admin', 'KQXMuEEpxthWmk75')
    response = requests.request(method, url, json=json, headers=headers, auth=auth)
    return response

def create_dag(config, name_dag):
    if st.button("Tạo dag"):
        if config:
            if 'config' in st.session_state:
                config_create_dag = json.loads(st.session_state.config)
                # URL để trigger DAG
                trigger_url = f"http://172.28.136.105:8080/api/v1/dags/CREATE_DAG/dagRuns"
                
                with st.spinner("Đang tạo DAG, vui lòng chờ..."):
                    response = api_request('POST', trigger_url, json={'conf': {'config': config_create_dag, 'name': name_dag}})

                    if response.status_code == 200:
                        dag_run_id = response.json().get('dag_run_id')
                        st.session_state.dag_run_id = dag_run_id
                        st.session_state.dag_created = True
                        st.success("Dag đang được tạo, vui lòng chờ trong khi dag được chạy.")
                    elif response.status_code == 403:
                        st.error("Access Denied: Check your credentials and permissions.")
                        st.session_state.dag_created = False
                    else:
                        st.error(f"Failed to trigger DAG. Status code: {response.status_code}, Response: {response.text}")
                        st.session_state.dag_created = False

def check_dag_run_status(dag_run_id):
    check_url = f"http://172.28.136.105:8080/api/v1/dags/CREATE_DAG/dagRuns/{dag_run_id}"
    
    with st.spinner("Đang kiểm tra trạng thái DAG..."):
        while True:
            response = api_request('GET', check_url)
            if response.status_code == 200:
                status = response.json().get('state')
                if status == 'success':
                    st.session_state.dag_ready = True
                    st.success("DAG đã sẵn sàng để chạy.")
                    return True
                elif status in ['failed', 'failed']:
                    st.session_state.dag_ready = False
                    st.error("DAG đã thất bại hoặc không thành công.")
                    return False
            time.sleep(3) 

def run_dag(name_dag):
    if 'dag_created' not in st.session_state or not st.session_state.dag_created:
        st.warning("Bạn cần tạo DAG trước khi chạy.")
        st.button("Chạy dag", disabled=True)
        return

    if st.button("Chạy dag"):
        if 'dag_ready' not in st.session_state or not st.session_state.dag_ready:
            st.error("DAG không sẵn sàng để chạy.")
            return
        
        trigger_url = f"http://172.28.136.105:8080/api/v1/dags/{name_dag}/dagRuns"
        
        auth = HTTPBasicAuth('admin', 'KQXMuEEpxthWmk75')
        
        csrf_url = f"http://172.28.136.105:8080/api/v1/dags/{name_dag}"
        csrf_token = get_csrf_token()  

        headers = {
            "Content-Type": "application/json",
            "X-CSRF-Token": csrf_token
        }
        
        with st.spinner("Đang chạy DAG, vui lòng chờ..."):
            # Gửi yêu cầu chạy DAG
            response = api_request('POST', trigger_url, json={})

            if response.status_code == 200:
                dag_run_id = response.json().get('dag_run_id')
                st.session_state.dag_run_id = dag_run_id
                st.success("Đang chạy DAG! Đang kiểm tra kết quả...")

                # Kiểm tra trạng thái của DAG
                check_url = f"http://172.28.136.105:8080/api/v1/dags/{name_dag}/dagRuns/{dag_run_id}"
                while True:
                    result_response = requests.get(check_url, headers=headers, auth=auth)
                    if result_response.status_code == 200:
                        status = result_response.json().get('state')
                        if status == 'success':
                            st.success("Chạy DAG thành công!")
                            st.write("Kết quả chi tiết:", result_response.json())
                            break
                        elif status in ['failed', 'failed']:
                            st.error("Chạy DAG thất bại. Xem chi tiết lỗi:")
                            st.write("Kết quả chi tiết:", result_response.json())
                            break
                    else:
                        st.error(f"Failed to retrieve DAG status. Status code: {result_response.status_code}, Response: {result_response.text}")
                        break
                    time.sleep(3) 
            elif response.status_code == 403:
                st.error("Access Denied: Check your credentials and permissions.")
            else:
                st.error(f"Failed to trigger DAG. Status code: {response.status_code}, Response: {response.text}")

def log_config():
    source_type = st.selectbox("Chọn loại nguồn dữ liệu", options=["Excel", "CSV", "SQL"])
    config =  None
    
    if source_type == "Excel":
        uploaded_file = st.file_uploader("Chọn một file", type=["xlsx"])
        if uploaded_file:
            try:
                file_name = uploaded_file.name
                output_dir = "/tmp/"
                os.makedirs(output_dir, exist_ok=True)
                file_path = os.path.join(output_dir, file_name)
                
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                @st.cache_data
                def load_data(uploaded_file, selected_sheet, header_input):
                    try:
                        return pd.read_excel(uploaded_file, sheet_name=selected_sheet, header=header_input)
                    except requests.exceptions.RequestException as e:
                        st.error(f"An error occurred: {e}")
                    except Exception as e:
                        st.error(f"An unexpected error occurred: {e}")
                        
                xls = pd.ExcelFile(uploaded_file)
                sheet_names = xls.sheet_names
                selected_sheet = st.selectbox("Chọn sheet để đọc dữ liệu", sheet_names)
                header_input = st.number_input("Chọn dòng làm header:", min_value=0, value=0)
                name_dag = 'test_create_dag'
                
                df = load_data(uploaded_file, selected_sheet, header_input)

                st.write("Dữ liệu trong sheet (50 bản ghi đầu tiên):")
                st.write(df.head(50))

                st.write("### Tạo bảng ánh xạ giữa các cột Excel và các cột trong cơ sở dữ liệu:")

                column_names = df.columns.tolist()
                column_mapping = {}
                
                if 'key_selections' not in st.session_state:
                    st.session_state.key_selections = []
                if 'column_mapping' not in st.session_state:
                    st.session_state.column_mapping = {}
                if 'submit_mapping_button' not in st.session_state:
                    st.session_state.submit_mapping_button = False
                if 'lookup_selections' not in st.session_state:
                    st.session_state.lookup_selections = []
                data_types = {}

                with st.form(key='mapping_form'):
                    for col in column_names:
                        db_column = st.text_input(f"Nhập tên cột trong cơ sở dữ liệu tương ứng với cột '{col}':")
                        data_type = st.selectbox(f"Chọn kiểu dữ liệu cho cột '{col}':", options=["", "string", "number", "date"], index=0)
                        if db_column:
                            column_mapping[col] = db_column
                            data_types[db_column] = data_type
                            
                    submit_mapping_button = st.form_submit_button("Xác nhận mapping để đẩy dữ liệu")
                    
                if submit_mapping_button:
                    st.session_state.submit_mapping_button = True
                    st.session_state.column_mapping = column_mapping

                if st.session_state.submit_mapping_button:
                    if st.session_state.column_mapping:
                        df = df[st.session_state.column_mapping.keys()]
                        df.rename(columns=st.session_state.column_mapping, inplace=True)
                        st.write("Dữ liệu sau khi áp dụng mapping:")
                        st.write(df)
                        
                        st.write("### Tạo key check trùng: ")
                        num_key = st.number_input("Nhập số lượng key để check trùng: ", min_value=0, max_value=len(df.columns), value=1)
                        
                        available_columns = list(df.columns)

                        # Đảm bảo key_selections trong session state có độ dài bằng num_key
                        if len(st.session_state.key_selections) < num_key:
                            st.session_state.key_selections.extend([''] * (num_key - len(st.session_state.key_selections)))

                        # Để mỗi key, cho phép người dùng chọn cột từ các cột có sẵn
                        for i in range(num_key):
                            if st.session_state.key_selections[i] in available_columns:
                                current_selection = st.session_state.key_selections[i]
                            else:
                                current_selection = available_columns[0]

                            key_selection = st.selectbox(f"Chọn cột làm key {i+1}:", available_columns, index=available_columns.index(current_selection), key=f"key_{i}")
                            st.session_state.key_selections[i] = key_selection
                            
                            # Loại bỏ cột đã chọn ra khỏi danh sách các cột có sẵn
                            available_columns.remove(key_selection)

                        st.write("Các cột được chọn làm key:", st.session_state.key_selections)
                    else:
                        st.warning("Không có cột nào được ánh xạ.")
                        
                config = LogConfigExcel(file_path, selected_sheet, data_types, column_mapping, header_input, st.session_state.key_selections, '', 'xlsx')
                
                create_dag(config=config, name_dag=name_dag)
                
                if 'dag_created' in st.session_state and st.session_state.dag_created:
                    check_dag_run_status(st.session_state.dag_run_id)
                    
                run_dag(name_dag=name_dag)
                
            except Exception as e:
                st.error(f"Đã xảy ra lỗi: {e}")

    elif source_type == "CSV":
        uploaded_file = st.file_uploader("Chọn một file CSV", type=["csv"])

        @st.cache_data
        def load_csv_data(uploaded_file, header_input):
            try:
                return pd.read_csv(uploaded_file, header=header_input, encoding='utf-8')
            except UnicodeDecodeError:
                st.warning("Không thể giải mã tệp bằng UTF-8. Thử sử dụng mã hóa 'ISO-8859-1'.")
                return pd.read_csv(uploaded_file, header=header_input, encoding='ISO-8859-1')

        if uploaded_file:
            try:
                header_input = st.number_input("Chọn dòng làm header:", min_value=0, value=0)
                df = load_csv_data(uploaded_file, header_input)
                name_dag = 'test_create_dag'

                st.write("Dữ liệu trong file CSV:")
                st.write(df)

                st.write("Tạo bảng ánh xạ giữa các cột CSV và các cột trong cơ sở dữ liệu:")

                column_names = df.columns.tolist()
                column_mapping_csv = {}
                if 'key_selections_csv' not in st.session_state:
                    st.session_state.key_selections_csv = []
                if 'column_mapping_csv' not in st.session_state:
                    st.session_state.column_mapping_csv = {}
                if 'submit_mapping_button_csv' not in st.session_state:
                    st.session_state.submit_mapping_button_csv = False
                data_types = {}
                
                with st.form(key='mapping_form'):
                    for col in column_names:
                        db_column = st.text_input(f"Nhập tên cột trong cơ sở dữ liệu tương ứng với cột '{col}':")
                        data_type = st.selectbox(f"Chọn kiểu dữ liệu cho cột '{col}':", options=["", "string", "number", "date"], index=0)
                        if db_column:
                            column_mapping_csv[col] = db_column
                            data_types[db_column] = data_type

                    submit_mapping_button_csv = st.form_submit_button("Xác nhận mapping để đẩy dữ liệu")
                
                if submit_mapping_button_csv:
                    st.session_state.submit_mapping_button_csv = True
                    st.session_state.column_mapping_csv = column_mapping_csv

                if st.session_state.submit_mapping_button_csv:
                    if st.session_state.column_mapping_csv:
                        df = df[st.session_state.column_mapping_csv.keys()]
                        df.rename(columns=st.session_state.column_mapping_csv, inplace=True)
                        st.write("Dữ liệu sau khi áp dụng mapping:")
                        st.write(df)
                        
                        num_key = st.number_input("Nhập số lượng key để check trùng: ", min_value=0, max_value=len(df.columns), value=1)
                        
                        available_columns = list(df.columns)

                        if len(st.session_state.key_selections_csv) < num_key:
                            st.session_state.key_selections_csv.extend([''] * (num_key - len(st.session_state.key_selections_csv)))

                        for i in range(num_key):
                            if st.session_state.key_selections_csv[i] in available_columns:
                                current_selection = st.session_state.key_selections_csv[i]
                            else:
                                current_selection = available_columns[0]

                            key_selection = st.selectbox(f"Chọn cột làm key {i+1}:", available_columns, index=available_columns.index(current_selection), key=f"key_{i}")
                            st.session_state.key_selections_csv[i] = key_selection
                            
                            available_columns.remove(key_selection)

                        st.write("Các cột được chọn làm key:", st.session_state.key_selections_csv)
                    else:
                        st.warning("Không có cột nào được ánh xạ.")

                config = LogConfigExcel(uploaded_file, "", data_types, column_mapping_csv, header_input, st.session_state.key_selections_csv,"csv")
                
                create_dag(config=config, name_dag=name_dag)
                
                if 'dag_created' in st.session_state and st.session_state.dag_created:
                    check_dag_run_status(st.session_state.dag_run_id)
                    
                run_dag(name_dag=name_dag)
                
            except Exception as e:
                st.error(f"Đã xảy ra lỗi: {e}")

    elif source_type == 'SQL':
        server = st.text_input("Nhập tên server nguồn:")
        port = st.text_input("Nhập số port nguồn:", value="1433")
        database = st.text_input("Nhập tên cơ sở dữ liệu nguồn:")
        username = st.text_input("Nhập tên đăng nhập nguồn:")
        password = st.text_input("Nhập mật khẩu nguồn:", type="password")
        table_name = st.text_input("Nhập tên bảng nguồn:")
        sql_query = st.text_area("Nhập truy vấn SQL (tùy chọn):")
        name_dag = 'test_create_dag'

        if st.button("Lấy dữ liệu"):
            try:
                conn_str = (
                    f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}"
                    "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
                )
                
                engine = create_engine(conn_str)
                
                with engine.connect() as conn:
                    if not sql_query.strip():
                        sql_query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(
                        sql=sql_query,
                        con=conn.connection
                    )
                    st.session_state['df'] = df
                    st.session_state['column_names'] = df.columns.tolist()

                    st.write("Dữ liệu từ cơ sở dữ liệu:")
                    st.write(df)
                    
                # df = pd.read_sql(sql_query, con=engine)
                
                # with engine.connect() as conn:
                #     if not sql_query.strip():
                #         sql_query = f"SELECT * FROM {table_name}"
                #     df = pd.read_sql(sql_query, conn)

                

            except Exception as e:
                st.error(f"Đã xảy ra lỗi khi thực hiện truy vấn: {e}")

        if 'df' in st.session_state:
            try:
                if 'column_mapping_sql' not in st.session_state:
                    st.session_state['column_mapping_sql'] = {}
                if 'data_types_sql' not in st.session_state:
                    st.session_state['data_types_sql'] = {}
                if 'key_selections_sql' not in st.session_state:
                    st.session_state['key_selections_sql'] = []
                if 'available_columns_sql' not in st.session_state:
                    st.session_state['available_columns_sql'] = st.session_state['column_names'].copy()
                if 'num_key_sql' not in st.session_state:
                    st.session_state['num_key_sql'] = 1
                    
                df = st.session_state['df']
                column_names = st.session_state['column_names']

                st.write("Tạo bảng ánh xạ giữa các cột SQL và các cột trong cơ sở dữ liệu:")
                if 'submit_mapping_button_sql' not in st.session_state:
                    st.session_state['submit_mapping_button_sql'] = False
                # Xử lý ánh xạ cột
                with st.form(key='mapping_form'):
                    for col in column_names:
                        db_column = st.text_input(f"Nhập tên cột trong cơ sở dữ liệu tương ứng với cột '{col}':", key=f"{col}_db_col")
                        data_type = st.selectbox(f"Chọn kiểu dữ liệu cho cột '{col}':", options=["", "string", "number", "date"], index=0, key=f"{col}_data_type")
                        if db_column:
                            st.session_state['column_mapping_sql'][col] = db_column
                            st.session_state['data_types_sql'][db_column] = data_type

                    submit_mapping_button = st.form_submit_button("Xác nhận mapping để đẩy dữ liệu")

                # Cập nhật trạng thái nút submit_mapping_button
                if submit_mapping_button:
                    st.session_state['submit_mapping_button_sql'] = True
                
                if st.session_state['submit_mapping_button_sql']:
                    if st.session_state['column_mapping_sql']:
                        df = df[list(st.session_state['column_mapping_sql'].keys())]
                        df.rename(columns=st.session_state['column_mapping_sql'], inplace=True)
                        st.write("Dữ liệu sau khi áp dụng mapping:")
                        st.write(df)
                        
                        # Cập nhật key_selections_sql dựa trên column_mapping_sql
                        st.session_state['available_columns_sql'] = list(st.session_state['column_mapping_sql'].values())
                        
                        num_key_sql = st.number_input("Nhập số lượng key để check trùng: ", min_value=1, max_value=len(st.session_state['available_columns_sql']), value=st.session_state['num_key_sql'])
                        st.session_state['num_key_sql'] = num_key_sql

                        if len(st.session_state['key_selections_sql']) < num_key_sql:
                            # Extend the list if necessary
                            st.session_state['key_selections_sql'].extend([None] * (num_key_sql - len(st.session_state['key_selections_sql'])))
                        elif len(st.session_state['key_selections_sql']) > num_key_sql:
                            # Trim the list if necessary
                            st.session_state['key_selections_sql'] = st.session_state['key_selections_sql'][:num_key_sql]

                        available_columns_sql = st.session_state['available_columns_sql'].copy()

                        # Render selectboxes for keys
                        for i in range(num_key_sql):
                            # Check if the selection is already made and the column is still available
                            if st.session_state['key_selections_sql'][i] in available_columns_sql:
                                selected_index = available_columns_sql.index(st.session_state['key_selections_sql'][i])
                            else:
                                selected_index = 0  # Default to the first option if not available

                            # Render the selectbox
                            key_selection = st.selectbox(
                                f"Chọn cột làm key {i+1}:",
                                available_columns_sql,
                                index=selected_index,
                                key=f"key_{i}"
                            )

                            # Update key_selections_sql
                            st.session_state['key_selections_sql'][i] = key_selection

                            # Remove the selected column from available_columns_sql
                            available_columns_sql = [col for col in available_columns_sql if col != key_selection]
            
            except requests.exceptions.RequestException as e:
                st.error(f"An error occurred: {e}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                
            config = LogConfigSQL(server, database, username, password, port, st.session_state['key_selections_sql'], table_name)
            
            create_dag(config=config, name_dag=name_dag)
                
            if 'dag_created' in st.session_state and st.session_state.dag_created:
                check_dag_run_status(st.session_state.dag_run_id)
                
            run_dag(name_dag=name_dag)

log_config()