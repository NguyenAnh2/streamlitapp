import streamlit as st
import json 

def LogConfigSQL(server, database, username, password, port, key, table_name):
    
    target_server = st.text_input("Nhập tên server đích:")
    port = st.text_input("Nhập số port đích (mặc định là 1433):", value="1433")
    target_database = st.text_input("Nhập tên cơ sở dữ liệu đích:")
    target_username = st.text_input("Nhập tên đăng nhập đích:")
    target_password = st.text_input("Nhập mật khẩu đích:", type="password")
    target_table = st.text_input("Nhập tên bảng đích để mapping:")
    
    config = None
    
    if st.button("Xuất ra cấu hình"):
        config = {
            "extractSource": [
                {
                    "type": "sqlserver",
                    "config": {
                        "server": server,
                        "database": database,
                        "username": username,
                        "password": password,
                        "table": table_name,
                        "port": port
                    }
                }
            ],
            "map": [
                {
                    "connection": {
                        "type": "sqlserver",
                        "config": {
                            "server": target_server,
                            "database": target_database,
                            "username": target_username,
                            "password": target_password,
                            "port": port,
                            "table": target_table,
                            "keys": key
                        }
                    },
                    "map": [
                        {
                            "source_in": [
                                {"sourceindex": 0, "field": sql_col}
                            ],
                            "field_out": db_col,
                            "transform": [
                                # Nếu xử lý transform dugnf đoạn code sau:
                                # {
                                #     "type": st.session_state['data_types'][db_col],
                                #     "config": {}
                                # }
                            ]
                        } for sql_col, db_col in st.session_state['column_mapping_sql'].items()
                    ]
                }
            ]
        }
        
        st.session_state.config = json.dumps(config)
        st.write("Cấu hình được xuất ra:")
        st.json(config)

    return st.session_state.config if 'config' in st.session_state else json.dumps({})
        
def LogConfigExcel(uploaded_file, selected_sheet, data_types, column_mapping, header_input, key, lookupinfo, type):
    target_server = st.text_input("Nhập tên server đích:")
    port = st.text_input("Nhập số port đích(mặc định là 1433):", value="1433")
    target_database = st.text_input("Nhập tên cơ sở dữ liệu đích:")
    target_username = st.text_input("Nhập tên đăng nhập đích:")
    target_password = st.text_input("Nhập mật khẩu đích:", type="password")
    target_table = st.text_input("Nhập tên bảng đích để mapping:")
    
    config = None
    
    if st.button("Xuất ra cấu hình"):
        if type == 'xlsx': 
            extractSounre = [
                {
                    "type": "xlsx",
                    "config": {
                        "file_path": f"{uploaded_file}",
                        "sheet_name": selected_sheet,
                        "skips_row": header_input,
                    }
                }
            ]
        elif type =='csv':
            extractSounre = [
                {
                    "type": "csv",
                    "config": {
                        "file_path": uploaded_file.name,
                        "skips_row": header_input,
                    }
                }
            ]
        
        config = {
            "extractSource": extractSounre,
            "map": [
                {
                    "connection": {
                        "type": "sqlserver",
                        "config": {
                            "server": target_server,
                            "database": target_database,
                            "username": target_username,
                            "password": target_password,
                            "port": port,
                            "table": target_table,
                            "keys": key
                        },
                        "lookupinfo": lookupinfo
                    },
                    "map": [
                        {
                            "source_in": [
                                {"sourceindex": 0, "field": excel_col}
                            ],
                            "field_out": db_col,
                            "transform": [
                                # Nếu xử lý transform dugnf đoạn code sau:
                                # {
                                #     "type": data_types[db_col],
                                #     "config": {}
                                # }
                            ]
                        } for excel_col, db_col in column_mapping.items()
                    ]
                }
            ]
        }
        
        st.session_state.config = json.dumps(config)
        st.write("Cấu hình được xuất ra:")
        st.json(config)
        
    return st.session_state.config if 'config' in st.session_state else json.dumps({})
    