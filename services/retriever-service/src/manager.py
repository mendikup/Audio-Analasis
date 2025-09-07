from dal.files_dal import Files_Loader

def run_all():

    files_loader = Files_Loader()
    files_loader.get_files_meta_data()
    files_loader.write_to_json_file()



